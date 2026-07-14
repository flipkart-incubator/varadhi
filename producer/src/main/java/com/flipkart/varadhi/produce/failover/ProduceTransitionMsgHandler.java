package com.flipkart.varadhi.produce.failover;

import com.flipkart.varadhi.common.utils.RetryUtils;
import com.flipkart.varadhi.core.ResourceReadCache;
import com.flipkart.varadhi.core.cluster.MsgHandler;
import com.flipkart.varadhi.core.cluster.controller.ControllerConsumerApi;
import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.entities.Resource;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.cluster.failover.TransitionAck;
import com.flipkart.varadhi.entities.cluster.failover.TransitionEvent;
import com.flipkart.varadhi.entities.cluster.failover.TransitionParticipation;
import com.flipkart.varadhi.entities.cluster.failover.TransitionStage;
import com.flipkart.varadhi.produce.ProducerService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Minimal pod-side handler for topic-transition stage broadcasts (topic failover and
 * storage-topic migration alike). It reacts using only the self-contained
 * {@link TransitionEvent} and the pod's local {@code TopicCache}.
 *
 * <p>Participation ({@link TransitionParticipation#INVOLVED} vs
 * {@link TransitionParticipation#NOT_INVOLVED}) and type-specific PREPARE warm live here so the
 * policy is owned and tested with the handler — not in wiring lambdas.
 *
 * <p><b>Every</b> stage is acknowledged. Version-gated stages wait for the local TopicCache to
 * converge to the <em>exact</em> coordinated version before acking; all others ack immediately
 * on receipt:
 * <ul>
 *   <li><b>PREPARE</b> ({@code topicVersionToAwait} = N) — readiness: poll until the cache
 *       observes exactly version N, then if this pod is already producing the topic pre-warm the
 *       target producer ({@link TransitionParticipation#INVOLVED}); otherwise stay
 *       {@link TransitionParticipation#NOT_INVOLVED} (no producer created; fencing can plug in
 *       later) and ack. A stale/unreachable pod times out, and a warm failure acks failure — both
 *       let the controller abort before any change.</li>
 *   <li><b>SWITCH</b> ({@code topicVersionToAwait} = N+1) — convergence: same exact-version
 *       wait but for N+1, then ack.</li>
 *   <li>For version-gated stages, if the cache has already moved <em>past</em> the target, the
 *       pod acks failure, treating it as a concurrent modification so the controller can
 *       abort/retry.</li>
 *   <li><b>PENDING / COMPLETED / ABORTED</b> ({@code awaitVersion} = false) — no version to
 *       await; ack immediately so the controller knows the pod processed the stage.</li>
 * </ul>
 */
@Slf4j
@AllArgsConstructor
public final class ProduceTransitionMsgHandler implements MsgHandler {

    private final String hostname;
    private final ResourceReadCache<Resource.EntityResource<VaradhiTopic>> topicCache;
    private final ControllerConsumerApi controllerClient;
    private final ProducerService producerService;
    private final PodTransitionConfig config;
    private final ScheduledExecutorService scheduler;
    private final TransitionMetrics metrics;

    @Override
    public void handle(ClusterMessage message) {
        TransitionEvent event = message.getData(TransitionEvent.class);
        metrics.stageReceived(event.transitionType(), event.stage());
        // Non-version-gated stages ack immediately on receipt.
        if (!event.awaitVersion()) {
            ackOk(event, null);
            return;
        }
        // handle() runs on the event-bus thread that delivered this publish. The version wait (and
        // the PREPARE pre-warm it triggers) runs on the transition scheduler via RetryUtils so the
        // event bus is never stalled. Polling uses a fixed interval (not exponential backoff): this
        // is cache convergence within a deadline, not failure retry.
        int maxAttempts = Math.max(1, (int)Math.ceil((double)config.podVersionWaitMs() / config.podPollIntervalMs()));
        metrics.versionWaitStarted();
        RetryUtils.getAsync(
            scheduler,
            maxAttempts,
            config.podPollIntervalMs(),
            Optional::isEmpty,
            () -> probeVersion(event)
        ).whenComplete((outcome, t) -> {
            metrics.versionWaitFinished();
            if (t != null) {
                if (RetryUtils.isRetriesExceeded(t)) {
                    ackFail(event, null, versionWaitTimeoutMessage(event));
                    return;
                }
                log.error("transition version wait failed for {} op={}", event.topicFqn().toFqn(), event.opId(), t);
                ackFail(event, null, "transition poll error: " + RetryUtils.rootMessage(t));
                return;
            }
            if (outcome.isEmpty()) {
                ackFail(event, null, versionWaitTimeoutMessage(event));
                return;
            }
            onVersionResolved(event, outcome.get());
        });
    }

    private String versionWaitTimeoutMessage(TransitionEvent event) {
        return "timeout awaiting topic version " + event.topicVersionToAwait() + " (current " + describe(
            currentVersion(event)
        ) + ")";
    }

    /**
     * Terminal when the cache has reached (or overshot) the coordinated version, empty while it is
     * still behind or absent (keep polling). Returns the observed version on termination.
     */
    private Optional<Long> probeVersion(TransitionEvent event) {
        Optional<Long> current = currentVersion(event);
        if (current.isEmpty()) {
            return Optional.empty();
        }
        long version = current.get();
        long target = event.topicVersionToAwait();
        if (version >= target) {
            return Optional.of(version);
        }
        return Optional.empty();
    }

    private void onVersionResolved(TransitionEvent event, long current) {
        if (current > event.topicVersionToAwait()) {
            // The cache jumped past the version the controller coordinated: the topic was modified
            // concurrently during the transition. Fail so the controller can abort/retry rather than
            // act on a version it never coordinated.
            ackFail(
                event,
                null,
                "topic version overshot target " + event.topicVersionToAwait() + " (current " + current
                       + "), concurrent modification"
            );
            return;
        }
        onVersionReached(event);
    }

    private Optional<Long> currentVersion(TransitionEvent event) {
        return topicCache.get(event.topicFqn().toFqn()).map(Resource::getVersion).map(Integer::longValue);
    }

    private void onVersionReached(TransitionEvent event) {
        // Only PREPARE runs participant work; every other version-gated stage just acks on convergence.
        if (event.stage() != TransitionStage.PREPARE) {
            ackOk(event, null);
            return;
        }
        if (!producerService.isProducingTopic(event.topicFqn())) {
            // NOT_INVOLVED: no warm. Local fencing can plug in here later.
            metrics.participation(event.transitionType(), TransitionParticipation.NOT_INVOLVED);
            log.debug(
                "Transition: pod not involved for {} op={} type={}; skipping participant work",
                event.topicFqn().toFqn(),
                event.opId(),
                event.transitionType()
            );
            ackOk(event, TransitionParticipation.NOT_INVOLVED);
            return;
        }
        metrics.participation(event.transitionType(), TransitionParticipation.INVOLVED);
        // INVOLVED: pre-warm the type-specific target so it is live before SWITCH. A warm failure
        // fails the ack, letting the controller abort before any switch. Async so producer creation
        // never blocks the scheduler thread.
        warmTarget(event).whenComplete((ignored, t) -> {
            if (t != null) {
                log.warn(
                    "Transition PREPARE warm failed for {} op={} type={}",
                    event.topicFqn().toFqn(),
                    event.opId(),
                    event.transitionType(),
                    t
                );
                ackFail(
                    event,
                    TransitionParticipation.INVOLVED,
                    "prepare warm failed: " + RetryUtils.rootMessage(t)
                );
                return;
            }
            ackOk(event, TransitionParticipation.INVOLVED);
        });
    }

    /**
     * Type-specific PREPARE warm for an {@link TransitionParticipation#INVOLVED} pod.
     * Parses {@link TransitionEvent#target()} once at the boundary via {@link PrepareTarget}.
     */
    private CompletableFuture<Void> warmTarget(TransitionEvent event) {
        final PrepareTarget prepareTarget;
        try {
            prepareTarget = PrepareTarget.parse(event.transitionType(), event.target());
        } catch (IllegalArgumentException e) {
            return CompletableFuture.failedFuture(e);
        }
        return switch (prepareTarget) {
            case PrepareTarget.RegionTarget regionTarget -> producerService.getProducer(
                event.topicFqn(),
                regionTarget.region()
            ).thenAccept(producer -> {});
            case PrepareTarget.StorageTopicTarget storageTarget -> producerService.getProducer(
                event.topicFqn(),
                storageTarget.storageTopicId()
            ).thenAccept(producer -> {});
        };
    }

    private static String describe(Optional<Long> version) {
        return version.map(Object::toString).orElse("absent from cache");
    }

    private void ackOk(TransitionEvent event, TransitionParticipation participation) {
        metrics.stageAcked(event.transitionType(), event.stage(), true);
        sendAck(
            TransitionAck.success(
                event.opId(),
                event.topicFqn(),
                event.transitionType(),
                participation,
                hostname,
                event.stage()
            )
        );
    }

    private void ackFail(TransitionEvent event, TransitionParticipation participation, String errorMsg) {
        metrics.stageAcked(event.transitionType(), event.stage(), false);
        String msg = (errorMsg == null || errorMsg.isBlank()) ? "transition stage failed" : errorMsg;
        sendAck(
            TransitionAck.failure(
                event.opId(),
                event.topicFqn(),
                event.transitionType(),
                participation,
                hostname,
                event.stage(),
                msg
            )
        );
    }

    private void sendAck(TransitionAck ack) {
        // Best-effort: if delivery fails, the controller stage barrier times out and re-pushes.
        controllerClient.ackTopicTransition(ack).exceptionally(t -> {
            metrics.ackSendFailed(ack.transitionType(), ack.stage());
            log.warn(
                "Failed to deliver transition ack op={} topic={} type={} stage={} host={} participation={}",
                ack.opId(),
                ack.topicFqn().toFqn(),
                ack.transitionType(),
                ack.stage(),
                ack.hostname(),
                ack.participation(),
                t
            );
            return null;
        });
    }
}
