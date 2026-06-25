package com.flipkart.varadhi.produce.failover;

import com.flipkart.varadhi.core.ResourceReadCache;
import com.flipkart.varadhi.core.cluster.MsgHandler;
import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.entities.Resource;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.cluster.failover.TransitionAck;
import com.flipkart.varadhi.entities.cluster.failover.TransitionEvent;
import com.flipkart.varadhi.entities.cluster.failover.TransitionStage;
import com.flipkart.varadhi.entities.cluster.failover.TransitionType;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

/**
 * Minimal pod-side handler for topic-transition stage broadcasts (topic failover and
 * storage-topic migration alike). It reacts using only the self-contained
 * {@link TransitionEvent} and the pod's local {@code TopicCache}; it never reads the
 * controller-side {@code TransitionObject}.
 *
 * <p>The stage machine and version-convergence logic are identical across
 * {@link TransitionType}s; only the PREPARE pre-warm differs and is supplied per type via
 * {@code prepareActions}. Each action takes {@code (topicFqn, target)} as opaque strings —
 * the action interprets {@code target} for its type (region for failover, storage-topic id
 * for migration) — and returns a {@link CompletableFuture} that completes when the producer
 * is warmed (or fails). Keeping it asynchronous means a producer cache-miss never blocks the
 * transition scheduler thread.
 *
 * <p><b>Every</b> stage is acknowledged. Version-gated stages wait for the local TopicCache to
 * converge to the <em>exact</em> coordinated version before acking; all others ack immediately
 * on receipt:
 * <ul>
 *   <li><b>PREPARE</b> ({@code topicVersionToAwait} = N) — readiness: poll until the cache
 *       observes exactly version N, then pre-warm this pod's producer and ack; a
 *       stale/unreachable pod times out, and a pod that cannot warm its producer acks
 *       failure — both let the controller abort before any change.</li>
 *   <li><b>SWITCH</b> ({@code topicVersionToAwait} = N+1) — convergence: same exact-version
 *       wait but for N+1, then ack.</li>
 *   <li>For version-gated stages, if the cache has already moved <em>past</em> the target, the
 *       pod acks failure, treating it as a concurrent modification so the controller can
 *       abort/retry.</li>
 *   <li><b>PENDING / DRAIN / COMPLETED / ABORTED</b> ({@code topicVersionToAwait} = 0) — no
 *       version to await; ack immediately so the controller knows the pod processed the stage.</li>
 * </ul>
 */
@Slf4j
@AllArgsConstructor
public final class ProduceTransitionMsgHandler implements MsgHandler {

    /** Sentinel for "topic not yet present in this pod's cache" (not the same as version 0). */
    private static final long VERSION_ABSENT = -1L;

    private final String hostname;
    private final ResourceReadCache<Resource.EntityResource<VaradhiTopic>> topicCache;
    private final TransitionAckClient ackClient;
    /**
     * PREPARE pre-warm action per {@link TransitionType}. Each accepts {@code (topicFqn, target)}
     * and asynchronously pre-creates the producer for the transition's target (region for
     * failover, storage-topic id for migration); the returned future fails if warming fails.
     */
    private final Map<TransitionType, BiFunction<String, String, CompletableFuture<Void>>> prepareActions;
    private final PodTransitionConfig config;
    private final ScheduledExecutorService scheduler;

    @Override
    public void handle(ClusterMessage message) {
        TransitionEvent event = message.getData(TransitionEvent.class);
        // Non-version-gated stages ack immediately on receipt.
        if (event.topicVersionToAwait() <= 0) {
            ackOk(event);
            return;
        }
        // handle() runs on the event-bus thread that delivered this publish. Offload the version
        // wait (and the PREPARE pre-warm it triggers) to the transition scheduler so the event bus
        // is never stalled.
        long deadlineInMs = System.currentTimeMillis() + config.podVersionWaitMs();
        scheduler.execute(() -> awaitVersionThenAck(event, deadlineInMs));
    }

    private void awaitVersionThenAck(TransitionEvent event, long deadlineInMs) {
        try {
            long current = currentVersion(event);
            long target = event.topicVersionToAwait();

            if (current == target) {
                onVersionReached(event);
                return;
            }
            if (current > target && current != VERSION_ABSENT) {
                // The cache jumped past the version the controller coordinated: the topic was
                // modified concurrently during the transition. Fail so the controller can
                // abort/retry rather than act on a version it never coordinated.
                ackFail(
                    event,
                    "topic version overshot target " + target + " (current " + current + "), concurrent modification"
                );
                return;
            }
            // Either the topic is not yet in this pod's cache (VERSION_ABSENT) or it has not yet
            // reached the target. Both are transient: keep polling until the deadline, then fail.
            if (System.currentTimeMillis() >= deadlineInMs) {
                ackFail(event, "timeout awaiting topic version " + target + " (current " + describe(current) + ")");
                return;
            }
            scheduler.schedule(
                () -> awaitVersionThenAck(event, deadlineInMs),
                config.podPollIntervalMs(),
                TimeUnit.MILLISECONDS
            );
        } catch (Exception e) {
            // Any unexpected failure in the poll loop must still notify the controller, otherwise
            // its stage barrier waits until timeout for an ack that will never come.
            log.error("transition poll loop failed for {} op={}", event.topicFqn(), event.opId(), e);
            ackFail(event, "transition poll error: " + e.getMessage());
        }
    }

    private long currentVersion(TransitionEvent event) {
        return topicCache.get(event.topicFqn())
                         .map(Resource::getVersion)
                         .map(Integer::longValue)
                         .orElse(VERSION_ABSENT);
    }

    private void onVersionReached(TransitionEvent event) {
        // Only PREPARE pre-warms; every other version-gated stage just acks on convergence.
        if (event.stage() != TransitionStage.PREPARE) {
            ackOk(event);
            return;
        }
        BiFunction<String, String, CompletableFuture<Void>> warmer = prepareActions.get(event.transitionType());
        if (warmer == null) {
            ackFail(event, "no prepare action registered for transition type " + event.transitionType());
            return;
        }
        // PREPARE doubles as readiness: pre-warm this pod's producer so the target has a live
        // producer before SWITCH flips produce to it. A warm failure fails the ack, letting the
        // controller abort before any switch. Done asynchronously so producer creation never
        // blocks the scheduler thread.
        warmer.apply(event.topicFqn(), event.target()).whenComplete((ignored, t) -> {
            if (t == null) {
                ackOk(event);
                return;
            }
            log.warn(
                "Transition PREPARE warm failed for {} op={} type={}",
                event.topicFqn(),
                event.opId(),
                event.transitionType(),
                t
            );
            ackFail(event, "prepare warm failed: " + t.getMessage());
        });
    }

    private static String describe(long version) {
        return version == VERSION_ABSENT ? "absent from cache" : Long.toString(version);
    }

    private void ackOk(TransitionEvent event) {
        ackClient.ack(TransitionAck.success(event.opId(), hostname, event.stage()));
    }

    private void ackFail(TransitionEvent event, String errorMsg) {
        ackClient.ack(TransitionAck.failure(event.opId(), hostname, event.stage(), errorMsg));
    }
}
