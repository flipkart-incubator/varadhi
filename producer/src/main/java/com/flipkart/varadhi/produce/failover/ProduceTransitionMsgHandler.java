package com.flipkart.varadhi.produce.failover;

import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.common.utils.RetryUtils;
import com.flipkart.varadhi.common.utils.ThrowableUtils;
import com.flipkart.varadhi.core.ResourceReadCache;
import com.flipkart.varadhi.core.cluster.MsgHandler;
import com.flipkart.varadhi.core.cluster.controller.TransitionApi;
import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.entities.Resource;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.VaradhiTopicName;
import com.flipkart.varadhi.entities.cluster.failover.TransitionAck;
import com.flipkart.varadhi.entities.cluster.failover.TransitionEvent;
import com.flipkart.varadhi.entities.cluster.failover.TransitionParticipation;
import com.flipkart.varadhi.entities.cluster.failover.TransitionStage;
import com.flipkart.varadhi.produce.ProducerService;
import dev.failsafe.FailsafeExecutor;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Minimal pod-side handler for topic-transition stage broadcasts (topic failover and
 * storage-topic migration alike). It reacts using only the self-contained
 * {@link TransitionEvent} and the pod's local {@code TopicCache}.
 *
 * <p>Participation ({@link TransitionParticipation#INVOLVED} vs
 * {@link TransitionParticipation#NOT_INVOLVED}) is decided at PREPARE and echoed on every
 * subsequent ack so the controller always knows pod involvement without an op-store lookup.
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
public final class ProduceTransitionMsgHandler implements MsgHandler {

    /** Failsafe probe outcome: {@link Pending} keeps polling; {@link Done} stops with the observed version. */
    private sealed interface VersionProbe permits VersionProbe.Pending, VersionProbe.Done {
        record Pending() implements VersionProbe {
        }

        record Done(long version) implements VersionProbe {
        }
    }

    private final String hostname;
    private final ResourceReadCache<Resource.EntityResource<VaradhiTopic>> topicCache;
    private final TransitionApi transitionApi;
    private final ProducerService producerService;
    private final TransitionMetrics metrics;
    private final FailsafeExecutor<VersionProbe> versionWaitExecutor;
    private final ConcurrentMap<String, TransitionParticipation> participationByOpId = new ConcurrentHashMap<>();

    public ProduceTransitionMsgHandler(
        String hostname,
        ResourceReadCache<Resource.EntityResource<VaradhiTopic>> topicCache,
        TransitionApi transitionApi,
        ProducerService producerService,
        PodTransitionConfig config,
        ScheduledExecutorService scheduler,
        TransitionMetrics metrics
    ) {
        this.hostname = hostname;
        this.topicCache = topicCache;
        this.transitionApi = transitionApi;
        this.producerService = producerService;
        this.metrics = metrics;
        this.versionWaitExecutor = RetryUtils.newPollingExecutor(
            scheduler,
            config.versionWaitMaxAttempts(),
            config.podPollIntervalMs(),
            probe -> probe instanceof VersionProbe.Pending
        );
    }

    @Override
    public void handle(ClusterMessage message) {
        TransitionEvent event = message.getData(TransitionEvent.class);
        metrics.stageReceived(event.transitionType(), event.stage());
        // Non-version-gated stages ack immediately on receipt.
        if (!event.awaitVersion()) {
            ackOk(event);
            return;
        }
        // handle() runs on the event-bus thread that delivered this publish. The version wait (and
        // the PREPARE pre-warm it triggers) runs on the transition scheduler via a reusable
        // FailsafeExecutor from RetryUtils so the event bus is never stalled and the retry policy
        // is not rebuilt per event. Polling uses a fixed interval (not exponential backoff): cache
        // convergence within a deadline, not failure retry.
        String topicFqn = event.topicFqn().toFqn();
        metrics.versionWaitStarted();
        long targetVersion = event.topicVersionToAwait();
        versionWaitExecutor.getAsync(() -> probeVersion(topicFqn, targetVersion)).whenComplete((outcome, t) -> {
            metrics.versionWaitFinished();
            if (t != null) {
                log.error("transition version wait failed for {} op={}", topicFqn, event.opId(), t);
                ackFail(event, "transition version wait failed: " + ThrowableUtils.rootMessage(t));
                return;
            }
            onVersionResolved(event, ((VersionProbe.Done)outcome).version());
        });
    }

    /**
     * {@link VersionProbe.Pending} while the cache is still behind the coordinated version (keep
     * polling). {@link VersionProbe.Done} when at or past the target. Topic missing from the cache
     * aborts immediately (Failsafe {@code abortOn}).
     */
    private VersionProbe probeVersion(String topicFqn, long targetVersion) {
        Optional<Resource.EntityResource<VaradhiTopic>> cached = topicCache.get(topicFqn);
        if (cached.isEmpty()) {
            throw new ResourceNotFoundException("Topic(%s) does not exist in topic cache.".formatted(topicFqn));
        }
        long version = cached.get().getVersion();
        if (version < targetVersion) {
            return new VersionProbe.Pending();
        }
        return new VersionProbe.Done(version);
    }

    private void onVersionResolved(TransitionEvent event, long current) {
        if (current > event.topicVersionToAwait()) {
            // The cache jumped past the version the controller coordinated: the topic was modified
            // concurrently during the transition. Fail so the controller can abort/retry rather than
            // act on a version it never coordinated.
            ackFail(
                event,
                "topic version overshot target " + event.topicVersionToAwait() + " (current " + current
                       + "), concurrent modification"
            );
            return;
        }
        Optional<Resource.EntityResource<VaradhiTopic>> cached = topicCache.get(event.topicFqn().toFqn());
        if (cached.isEmpty()) {
            ackFail(event, "topic absent from cache after version convergence");
            return;
        }
        onVersionReached(event, cached.get().getEntity());
    }

    private void onVersionReached(TransitionEvent event, VaradhiTopic topic) {
        // Only PREPARE runs participant work; every other version-gated stage just acks on convergence.
        if (event.stage() != TransitionStage.PREPARE) {
            ackOk(event);
            return;
        }
        if (!hasActiveProducer(topic)) {
            TransitionParticipation participation = TransitionParticipation.NOT_INVOLVED;
            recordParticipation(event, participation);
            log.debug(
                "Transition: pod not involved for {} op={} type={}; skipping participant work",
                event.topicFqn().toFqn(),
                event.opId(),
                event.transitionType()
            );
            ackOk(event, participation);
            return;
        }
        TransitionParticipation participation = TransitionParticipation.INVOLVED;
        recordParticipation(event, participation);
        createTarget(event, topic).whenComplete((ignored, t) -> {
            if (t != null) {
                log.warn(
                    "Transition PREPARE warm failed for {} op={} type={}",
                    event.topicFqn().toFqn(),
                    event.opId(),
                    event.transitionType(),
                    t
                );
                ackFail(event, participation, "prepare warm failed: " + ThrowableUtils.rootMessage(t));
                return;
            }
            ackOk(event, participation);
        });
    }

    /**
     * Type-specific PREPARE warm for an {@link TransitionParticipation#INVOLVED} pod.
     */
    private CompletableFuture<Void> createTarget(TransitionEvent event, VaradhiTopic topic) {
        TransitionEvent.Target target = event.target();
        if (target instanceof TransitionEvent.Target.Region regionTarget) {
            return producerService.getProducerForRegion(topic, regionTarget.region()).thenAccept(producer -> {});
        }
        if (target instanceof TransitionEvent.Target.StorageTopic storageTarget) {
            return producerService.loadProducer(event.topicFqn(), storageTarget.storageTopicId());
        }
        return CompletableFuture.failedFuture(new IllegalStateException("PREPARE target missing"));
    }

    private void recordParticipation(TransitionEvent event, TransitionParticipation participation) {
        participationByOpId.put(event.opId(), participation);
        metrics.setParticipation(event.transitionType(), participation);
    }

    /**
     * Participation decided at PREPARE and sticky for the op. Late joiners (first event not PREPARE)
     * derive from whether this pod already has a producer for the topic's active produce target.
     */
    private TransitionParticipation resolveParticipation(TransitionEvent event) {
        return participationByOpId.computeIfAbsent(event.opId(), ignored -> deriveParticipation(event.topicFqn()));
    }

    private TransitionParticipation deriveParticipation(VaradhiTopicName topicFqn) {
        return topicCache.get(topicFqn.toFqn())
                         .map(Resource.EntityResource::getEntity)
                         .filter(this::hasActiveProducer)
                         .map(ignored -> TransitionParticipation.INVOLVED)
                         .orElse(TransitionParticipation.NOT_INVOLVED);
    }

    /** True when the producer cache holds the active produce key for this pod's deployed region. */
    private boolean hasActiveProducer(VaradhiTopic topic) {
        return topic.resolveProduceTarget(producerService.deployedRegion())
                    .map(
                        target -> producerService.hasProducer(
                            topic.getName(),
                            target.storageTopic().getId(),
                            target.produceRegion().value()
                        )
                    )
                    .orElse(false);
    }

    private void clearParticipationIfTerminal(TransitionEvent event) {
        if (event.stage() == TransitionStage.COMPLETED || event.stage() == TransitionStage.ABORTED) {
            participationByOpId.remove(event.opId());
            metrics.clearParticipation(event.transitionType());
        }
    }

    private void ackOk(TransitionEvent event) {
        ackOk(event, resolveParticipation(event));
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
        clearParticipationIfTerminal(event);
    }

    private void ackFail(TransitionEvent event, String errorMsg) {
        ackFail(event, resolveParticipation(event), errorMsg);
    }

    private void ackFail(TransitionEvent event, TransitionParticipation participation, String errorMsg) {
        metrics.stageAcked(event.transitionType(), event.stage(), false);
        sendAck(
            TransitionAck.failure(
                event.opId(),
                event.topicFqn(),
                event.transitionType(),
                participation,
                hostname,
                event.stage(),
                errorMsg
            )
        );
        clearParticipationIfTerminal(event);
    }

    private void sendAck(TransitionAck ack) {
        // Best-effort: if delivery fails, the controller stage barrier times out and re-pushes.
        transitionApi.ack(ack).exceptionally(t -> {
            metrics.ackSendFailed(ack.transitionType(), ack.stage());
            log.warn("Failed to deliver transition ack ack={}", ack, t);
            return null;
        });
    }
}
