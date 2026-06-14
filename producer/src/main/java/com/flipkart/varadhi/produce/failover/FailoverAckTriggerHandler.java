package com.flipkart.varadhi.produce.failover;

import com.flipkart.varadhi.core.ResourceReadCache;
import com.flipkart.varadhi.core.cluster.MsgHandler;
import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.entities.Resource;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.cluster.failover.FailoverStageEvent;
import com.flipkart.varadhi.entities.cluster.failover.FailoverStatusUpdate;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Minimal pod-side handler for failover stage broadcasts. It reacts using only the
 * self-contained {@link FailoverStageEvent} and the pod's local {@code TopicCache};
 * it never reads the controller-side {@code TransitionObject}.
 *
 * <p><b>Every</b> stage is acknowledged. Version-gated stages wait for the local
 * TopicCache to converge before acking; all others ack immediately on receipt:
 * <ul>
 *   <li><b>PREPARE</b> ({@code topicVersionToAwait} = N) — readiness: poll the local
 *       TopicCache until it reaches version N, then ack; a stale/unreachable pod times
 *       out and acks failure, letting the controller abort before any change.</li>
 *   <li><b>SWITCH</b> ({@code topicVersionToAwait} = N+1) — convergence: same
 *       version-wait but for N+1, then ack.</li>
 *   <li><b>PENDING / DRAIN / COMPLETED / ABORTED</b> ({@code topicVersionToAwait} = 0) —
 *       no version to await; ack immediately so the controller knows the pod processed
 *       the stage.</li>
 * </ul>
 *
 * <p>The produce path itself is unchanged: once the TopicCache reflects the new
 * per-region {@code TopicState}, {@code ProducerService} gates produce automatically.
 */
@Slf4j
public final class FailoverAckTriggerHandler implements MsgHandler {

    private final String hostname;
    private final ResourceReadCache<Resource.EntityResource<VaradhiTopic>> topicCache;
    private final FailoverAcker ackClient;
    private final PodFailoverConfig config;
    private final ScheduledExecutorService scheduler;

    public FailoverAckTriggerHandler(
        String hostname,
        ResourceReadCache<Resource.EntityResource<VaradhiTopic>> topicCache,
        FailoverAcker ackClient,
        PodFailoverConfig config,
        ScheduledExecutorService scheduler
    ) {
        this.hostname = hostname;
        this.topicCache = topicCache;
        this.ackClient = ackClient;
        this.config = config;
        this.scheduler = scheduler;
    }

    @Override
    public void handle(ClusterMessage message) {
        FailoverStageEvent event = message.getData(FailoverStageEvent.class);
        // Every stage is acked. Version-gated stages (PREPARE=N, SWITCH=N+1) first wait for
        // the TopicCache to converge; all others ack immediately on receipt.
        if (event.topicVersionToAwait() > 0) {
            awaitVersionThenAck(event, System.currentTimeMillis() + config.podSwitchWaitMs());
        } else {
            ackOk(event);
        }
    }

    private void awaitVersionThenAck(FailoverStageEvent event, long deadlineMs) {
        long current = topicCache.get(event.topicFqn()).map(Resource::getVersion).map(Integer::longValue).orElse(-1L);
        if (current >= event.topicVersionToAwait()) {
            ackOk(event);
            return;
        }
        if (System.currentTimeMillis() >= deadlineMs) {
            ackFail(
                event,
                "timeout awaiting topic version " + event.topicVersionToAwait() + " (current " + current + ")"
            );
            return;
        }
        scheduler.schedule(
            () -> awaitVersionThenAck(event, deadlineMs),
            config.podPollIntervalMs(),
            TimeUnit.MILLISECONDS
        );
    }

    private void ackOk(FailoverStageEvent event) {
        ackClient.ack(FailoverStatusUpdate.success(event.opId(), hostname, event.stage()));
    }

    private void ackFail(FailoverStageEvent event, String errorMsg) {
        ackClient.ack(FailoverStatusUpdate.failure(event.opId(), hostname, event.stage(), errorMsg));
    }
}
