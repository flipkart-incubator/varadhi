package com.flipkart.varadhi.produce.failover;

import com.flipkart.varadhi.common.utils.RetryUtils;
import com.flipkart.varadhi.core.ResourceReadCache;
import com.flipkart.varadhi.core.cluster.MsgHandler;
import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.entities.Resource;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.VaradhiTopicName;
import com.flipkart.varadhi.entities.cluster.failover.TransitionAck;
import com.flipkart.varadhi.entities.cluster.failover.TransitionEvent;
import com.flipkart.varadhi.entities.cluster.failover.TransitionStage;
import com.flipkart.varadhi.entities.cluster.failover.TransitionType;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;

/**
 * Minimal pod-side handler for topic-transition stage broadcasts (topic failover and
 * storage-topic migration alike). It reacts using only the self-contained
 * {@link TransitionEvent} and the pod's local {@code TopicCache};
 *
 * <p>The stage machine and version-convergence logic are identical across
 * {@link TransitionType}s; only the PREPARE pre-warm differs and is supplied per type via
 * {@code prepareActions}. Each action takes {@code (topicFqn, target)} — a typed
 * {@link VaradhiTopicName} and an opaque {@code target} string the action interprets for its
 * type (region for failover, storage-topic id for migration) — and returns a
 * {@link CompletableFuture} that completes when the producer is warmed (or fails). Keeping it
 * asynchronous means a producer cache-miss never blocks the transition scheduler thread.
 *
 * <p><b>Every</b> stage is acknowledged. Version-gated stages wait for the local TopicCache to
 * converge to the <em>exact</em> coordinated version before acking; all others ack immediately
 * on receipt:
 * <ul>
 *   <li><b>PREPARE</b> ({@code topicVersionToAwait} = N) — readiness: poll until the cache
 *       observes exactly version N, then pre-warm this pod's producer (only if the pod is already
 *       producing the topic; otherwise it stays {@link TransitionPrepareResult#NOT_INVOLVED} and
 *       creates nothing) and ack; a stale/unreachable pod times out, and a pod that cannot warm its
 *       producer acks failure — both let the controller abort before any change.</li>
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
    private final TransitionAckClient ackClient;
    /**
     * PREPARE pre-warm action per {@link TransitionType}. Each accepts {@code (topicFqn, target)}
     * and asynchronously prepares this pod for the transition's target (region for failover,
     * storage-topic id for migration). It returns {@link TransitionPrepareResult#INVOLVED} when the
     * pod was producing the topic and pre-created the producer, or
     * {@link TransitionPrepareResult#NOT_INVOLVED} when the pod has no producer for the topic and
     * therefore creates nothing; the returned future fails if warming fails.
     */
    private final Map<TransitionType, BiFunction<VaradhiTopicName, String, CompletableFuture<TransitionPrepareResult>>> prepareActions;
    private final PodTransitionConfig config;
    private final ScheduledExecutorService scheduler;
    private final TransitionMetrics metrics;

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
        // the PREPARE pre-warm it triggers) runs on the transition scheduler via RetryUtils so the
        // event bus is never stalled.
        int maxAttempts = Math.max(1, (int)Math.ceil((double)config.podVersionWaitMs() / config.podPollIntervalMs()));
        RetryUtils.getAsync(
            scheduler,
            maxAttempts,
            config.podPollIntervalMs(),
            Optional::isEmpty,
            () -> probeVersion(event)
        ).whenComplete((outcome, t) -> {
            if (t != null) {
                if (RetryUtils.isRetriesExceeded(t)) {
                    ackFail(event, versionWaitTimeoutMessage(event));
                    return;
                }
                log.error("transition version wait failed for {} op={}", event.topicFqn().toFqn(), event.opId(), t);
                ackFail(event, "transition poll error: " + RetryUtils.rootMessage(t));
                return;
            }
            if (outcome.isEmpty()) {
                ackFail(event, versionWaitTimeoutMessage(event));
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
        // Only PREPARE pre-warms; every other version-gated stage just acks on convergence.
        if (event.stage() != TransitionStage.PREPARE) {
            ackOk(event);
            return;
        }
        BiFunction<VaradhiTopicName, String, CompletableFuture<TransitionPrepareResult>> warmer = prepareActions.get(
            event.transitionType()
        );
        if (warmer == null) {
            ackFail(event, "no prepare action registered for transition type " + event.transitionType());
            return;
        }
        // PREPARE doubles as readiness: pods already producing this topic pre-warm the target
        // producer so it is live before SWITCH; pods not producing it stay NOT_INVOLVED and create
        // nothing (no unnecessary producers), still acking so the controller barrier completes. A
        // warm failure fails the ack, letting the controller abort before any switch. Done
        // asynchronously so producer creation never blocks the scheduler thread.
        warmer.apply(event.topicFqn(), event.target()).whenComplete((result, t) -> {
            if (t != null) {
                log.warn(
                    "Transition PREPARE warm failed for {} op={} type={}",
                    event.topicFqn().toFqn(),
                    event.opId(),
                    event.transitionType(),
                    t
                );
                ackFail(event, "prepare warm failed: " + t.getMessage());
                return;
            }
            if (result == TransitionPrepareResult.NOT_INVOLVED) {
                metrics.prepareNotInvolved(event.transitionType());
                log.debug(
                    "Transition PREPARE: pod not involved for {} op={} type={}; nothing to pre-warm",
                    event.topicFqn().toFqn(),
                    event.opId(),
                    event.transitionType()
                );
            }
            ackOk(event);
        });
    }

    private static String describe(Optional<Long> version) {
        return version.map(Object::toString).orElse("absent from cache");
    }

    private void ackOk(TransitionEvent event) {
        metrics.stageAcked(event.transitionType(), event.stage(), true);
        ackClient.ack(TransitionAck.success(event.opId(), hostname, event.stage()));
    }

    private void ackFail(TransitionEvent event, String errorMsg) {
        metrics.stageAcked(event.transitionType(), event.stage(), false);
        String msg = (errorMsg == null || errorMsg.isBlank()) ? "transition stage failed" : errorMsg;
        ackClient.ack(TransitionAck.failure(event.opId(), hostname, event.stage(), msg));
    }
}
