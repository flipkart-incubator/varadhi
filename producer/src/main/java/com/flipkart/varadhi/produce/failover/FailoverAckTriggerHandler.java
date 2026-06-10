package com.flipkart.varadhi.produce.failover;

import com.flipkart.varadhi.core.ResourceReadCache;
import com.flipkart.varadhi.core.cluster.MsgHandler;
import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.entities.Resource;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.cluster.failover.FailoverStageEvent;
import com.flipkart.varadhi.entities.cluster.failover.FailoverStatusUpdate;
import com.flipkart.varadhi.spi.services.BrokerWarmer;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Minimal pod-side handler for failover stage broadcasts. It reacts using only the
 * self-contained {@link FailoverStageEvent} and the pod's local {@code TopicCache};
 * it never reads the controller-side {@code TransitionObject}.
 *
 * <ul>
 *   <li><b>PREPARE</b> — if this pod is in the target region, pre-warm the producer via
 *       {@link BrokerWarmer}, then ack; other pods ack immediately.</li>
 *   <li><b>SWITCH</b> — poll the local TopicCache until it reaches the broadcast
 *       {@code topicVersionToAwait} (= N+1), then ack; ack failure on timeout.</li>
 *   <li><b>terminal / other</b> — nothing to apply; no ack (controller isn't waiting).</li>
 * </ul>
 *
 * <p>The produce path itself is unchanged: once the TopicCache reflects the new
 * per-region {@code TopicState}, {@code ProducerService} gates produce automatically.
 */
@Slf4j
public final class FailoverAckTriggerHandler implements MsgHandler {

    private final String hostname;
    private final String localRegion;
    private final ResourceReadCache<Resource.EntityResource<VaradhiTopic>> topicCache;
    private final BrokerWarmer brokerWarmer;
    private final FailoverAcker ackClient;
    private final PodFailoverConfig config;
    private final ScheduledExecutorService scheduler;

    public FailoverAckTriggerHandler(
        String hostname,
        String localRegion,
        ResourceReadCache<Resource.EntityResource<VaradhiTopic>> topicCache,
        BrokerWarmer brokerWarmer,
        FailoverAcker ackClient,
        PodFailoverConfig config,
        ScheduledExecutorService scheduler
    ) {
        this.hostname = hostname;
        this.localRegion = localRegion;
        this.topicCache = topicCache;
        this.brokerWarmer = brokerWarmer;
        this.ackClient = ackClient;
        this.config = config;
        this.scheduler = scheduler;
    }

    @Override
    public void handle(ClusterMessage message) {
        FailoverStageEvent event = message.getData(FailoverStageEvent.class);
        switch (event.stage()) {
            case PREPARE -> handlePrepare(event);
            case SWITCH -> handleSwitch(event, System.currentTimeMillis() + config.podSwitchWaitMs());
            default -> log.debug("Ignoring non-actionable failover stage {} for {}", event.stage(), event.topicFqn());
        }
    }

    private void handlePrepare(FailoverStageEvent event) {
        if (Objects.equals(localRegion, event.targetRegion())) {
            brokerWarmer.warm(event.topicFqn(), event.targetRegion()).whenComplete((v, t) -> {
                if (t == null) {
                    ackOk(event);
                } else {
                    log.warn("PREPARE warm failed for {} on {}: {}", event.topicFqn(), hostname, t.getMessage());
                    ackFail(event, "warm failed: " + t.getMessage());
                }
            });
        } else {
            // Not the target region — nothing to warm, but still confirm the stage.
            ackOk(event);
        }
    }

    private void handleSwitch(FailoverStageEvent event, long deadlineMs) {
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
        scheduler.schedule(() -> handleSwitch(event, deadlineMs), config.podPollIntervalMs(), TimeUnit.MILLISECONDS);
    }

    private void ackOk(FailoverStageEvent event) {
        ackClient.ack(FailoverStatusUpdate.success(event.opId(), hostname, event.stage(), event.fenceVersion()));
    }

    private void ackFail(FailoverStageEvent event, String errorMsg) {
        ackClient.ack(
            FailoverStatusUpdate.failure(event.opId(), hostname, event.stage(), event.fenceVersion(), errorMsg)
        );
    }
}
