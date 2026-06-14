package com.flipkart.varadhi.produce.failover;

import com.flipkart.varadhi.core.ResourceReadCache;
import com.flipkart.varadhi.core.cluster.MsgHandler;
import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.entities.Resource;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.cluster.failover.FailoverAck;
import com.flipkart.varadhi.entities.cluster.failover.FailoverEvent;
import com.flipkart.varadhi.entities.cluster.failover.FailoverStage;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * Minimal pod-side handler for failover stage broadcasts. It reacts using only the
 * self-contained {@link FailoverEvent} and the pod's local {@code TopicCache};
 * it never reads the controller-side {@code TransitionObject}.
 *
 * <p><b>Every</b> stage is acknowledged. Version-gated stages wait for the local
 * TopicCache to converge to the <em>exact</em> coordinated version before acking; all
 * others ack immediately on receipt:
 * <ul>
 *   <li><b>PREPARE</b> ({@code topicVersionToAwait} = N) — readiness: poll the local
 *       TopicCache until it observes exactly version N, then pre-warm this pod's producer
 *       and ack; a stale/unreachable pod times out, and a pod that cannot warm its producer
 *       acks failure — both let the controller abort before any change.</li>
 *   <li><b>SWITCH</b> ({@code topicVersionToAwait} = N+1) — convergence: same
 *       exact-version wait but for N+1, then ack.</li>
 *   <li>For version-gated stages, if the cache has already moved <em>past</em> the target
 *       (a skipped version), the pod acks failure, treating it as a concurrent
 *       modification so the controller can abort/retry.</li>
 *   <li><b>PENDING / DRAIN / COMPLETED / ABORTED</b> ({@code topicVersionToAwait} = 0) —
 *       no version to await; ack immediately so the controller knows the pod processed
 *       the stage.</li>
 * </ul>
 *
 * <p>The produce path itself is unchanged: once the TopicCache reflects the new
 * per-region {@code TopicState}, {@code ProducerService} gates produce automatically.
 */
@Slf4j
@AllArgsConstructor
public final class FailoverAckTriggerHandler implements MsgHandler {

    private final String hostname;
    private final ResourceReadCache<Resource.EntityResource<VaradhiTopic>> topicCache;
    private final FailoverAckClient ackClient;
    /** Pre-warms a (topicFqn, targetRegion) producer (ProducerService::warmProducer); throws on failure. */
    private final BiConsumer<String, String> producerWarmer;
    private final PodFailoverConfig config;
    private final ScheduledExecutorService scheduler;

    @Override
    public void handle(ClusterMessage message) {
        FailoverEvent event = message.getData(FailoverEvent.class);
        // Every stage is acked. Version-gated stages (PREPARE=N, SWITCH=N+1) first wait for
        // the TopicCache to converge; all others ack immediately on receipt.
        if (event.topicVersionToAwait() > 0) {
            // handle() runs on the event-bus thread that delivered this publish. Offload the
            // version wait to the failover scheduler (the same thread the retry path uses) so
            // that a first-poll hit — which synchronously pre-warms the producer via a blocking
            // getProducer(...).join() — cannot stall the event bus on a producer cache miss.
            long deadlineMs = System.currentTimeMillis() + config.podSwitchWaitMs();
            scheduler.execute(() -> awaitVersionThenAck(event, deadlineMs)); 
        } else {
            ackOk(event);
        }
    }

    private void awaitVersionThenAck(FailoverEvent event, long deadlineMs) {
        long current = topicCache.get(event.topicFqn()).map(Resource::getVersion).map(Integer::longValue).orElse(-1L);
        long target = event.topicVersionToAwait();
        if (current == target) {
            onVersionReached(event);
            return;
        }
        if (current > target) {
            // The cache jumped past the version the controller coordinated: the topic was
            // modified concurrently during failover. Fail so the controller can abort/retry
            // rather than act on a version it never coordinated.
            ackFail(
                event,
                "topic version overshot target " + target + " (current " + current + "), concurrent modification"
            );
            return;
        }
        if (System.currentTimeMillis() >= deadlineMs) {
            ackFail(event, "timeout awaiting topic version " + target + " (current " + current + ")");
            return;
        }
        scheduler.schedule(
            () -> awaitVersionThenAck(event, deadlineMs),
            config.podPollIntervalMs(),
            TimeUnit.MILLISECONDS
        );
    }

    private void onVersionReached(FailoverEvent event) {
        // PREPARE doubles as readiness: pre-warm this pod's producer so the target region has a
        // live producer before SWITCH flips its TopicState to producing. A warm failure fails the
        // ack, letting the controller abort before any switch.
        if (event.stage() == FailoverStage.PREPARE) {
            try {
                producerWarmer.accept(event.topicFqn(), event.targetRegion());
            } catch (Exception e) {
                log.warn(
                    "Failover PREPARE warm failed for {} op={}: {}",
                    event.topicFqn(),
                    event.opId(),
                    e.getMessage()
                );
                ackFail(event, "prepare warm failed: " + e.getMessage());
                return;
            }
        }
        ackOk(event);
    }

    private void ackOk(FailoverEvent event) {
        ackClient.ack(FailoverAck.success(event.opId(), hostname, event.stage()));
    }

    private void ackFail(FailoverEvent event, String errorMsg) {
        ackClient.ack(FailoverAck.failure(event.opId(), hostname, event.stage(), errorMsg));
    }
}
