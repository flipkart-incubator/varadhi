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

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Pod-side handler for failover stage broadcasts. Single small class (~ the whole new
 * pod-side surface) that:
 *
 * <ol>
 *   <li>On {@link com.flipkart.varadhi.entities.cluster.failover.TriggerKind#PREPARE_HINT}: asks the
 *       {@link BrokerWarmer} to open / pre-fetch target-region producer state, then acks.</li>
 *   <li>On {@link com.flipkart.varadhi.entities.cluster.failover.TriggerKind#ACK_TRIGGER}: waits (up to
 *       {@code podSwitchWaitMs}) for the local L1 topic cache to advance to the
 *       {@code topicVersionToAwait} carried on the event, then acks.</li>
 *   <li>On {@link com.flipkart.varadhi.entities.cluster.failover.TriggerKind#TERMINAL}: no-op (purely
 *       informational; pods may drop any per-op state).</li>
 * </ol>
 *
 * <p>The actual produce-gating is handled entirely by the existing
 * {@code SegmentedStorageTopic.topicState} read by {@code ProducerService};
 * this handler does <b>not</b> mutate any shared state on the pod.
 */
@Slf4j
public final class FailoverAckTriggerHandler implements MsgHandler {

    private final String hostname;
    private final ResourceReadCache<Resource.EntityResource<VaradhiTopic>> topicCache;
    private final ControllerFailoverClient controllerClient;
    private final BrokerWarmer brokerWarmer;
    private final ScheduledExecutorService scheduler;
    private final long podSwitchWaitMs;
    private final long podPollIntervalMs;

    public FailoverAckTriggerHandler(
        String hostname,
        ResourceReadCache<Resource.EntityResource<VaradhiTopic>> topicCache,
        ControllerFailoverClient controllerClient,
        BrokerWarmer brokerWarmer,
        ScheduledExecutorService scheduler,
        long podSwitchWaitMs,
        long podPollIntervalMs
    ) {
        this.hostname = hostname;
        this.topicCache = topicCache;
        this.controllerClient = controllerClient;
        this.brokerWarmer = brokerWarmer == null ? BrokerWarmer.NO_OP : brokerWarmer;
        this.scheduler = scheduler;
        this.podSwitchWaitMs = podSwitchWaitMs;
        this.podPollIntervalMs = podPollIntervalMs;
    }

    @Override
    public void handle(ClusterMessage message) {
        FailoverStageEvent event = message.getData(FailoverStageEvent.class);
        try {
            switch (event.getKind()) {
                case PREPARE_HINT -> handlePrepare(event);
                case ACK_TRIGGER -> handleAckTrigger(event);
                case TERMINAL -> log.debug("Received terminal failover event {}", event);
            }
        } catch (Exception e) {
            log.error("Failover handler crashed for event {}: {}", event, e.getMessage(), e);
            controllerClient.ack(
                FailoverStatusUpdate.failure(
                    event.getOpId(),
                    hostname,
                    event.getStage(),
                    event.getFenceVersion(),
                    "pod-handler-crash: " + e.getMessage()
                )
            );
        }
    }

    private void handlePrepare(FailoverStageEvent event) {
        // We deliberately ack PREPARE optimistically: a warm-up failure is non-fatal
        // (the first produce after SWITCH may simply be slower), so we don't want
        // to stall the entire failover on a warm-up hiccup.
        brokerWarmer.warm(event.getParentFqn(), /*targetRegion is unknown to pod*/ resolveTargetRegion(event))
                    .whenComplete((v, t) -> {
                        if (t != null) {
                            log.warn("Broker warm-up failed for {}: {}", event.getParentFqn(), t.getMessage());
                        }
                        controllerClient.ack(
                            FailoverStatusUpdate.success(
                                event.getOpId(),
                                hostname,
                                event.getStage(),
                                event.getFenceVersion()
                            )
                        );
                    });
    }

    private void handleAckTrigger(FailoverStageEvent event) {
        int target = event.getTopicVersionToAwait();
        if (target < 0) {
            // Malformed event; ack-fail rather than block forever.
            controllerClient.ack(
                FailoverStatusUpdate.failure(
                    event.getOpId(),
                    hostname,
                    event.getStage(),
                    event.getFenceVersion(),
                    "ACK_TRIGGER event missing topicVersionToAwait"
                )
            );
            return;
        }
        long deadline = System.currentTimeMillis() + podSwitchWaitMs;
        pollUntilVersion(event, target, deadline);
    }

    private void pollUntilVersion(FailoverStageEvent event, int targetVersion, long deadlineMs) {
        if (cacheReached(event.getParentFqn(), targetVersion)) {
            controllerClient.ack(
                FailoverStatusUpdate.success(event.getOpId(), hostname, event.getStage(), event.getFenceVersion())
            );
            return;
        }
        if (System.currentTimeMillis() >= deadlineMs) {
            controllerClient.ack(
                FailoverStatusUpdate.failure(
                    event.getOpId(),
                    hostname,
                    event.getStage(),
                    event.getFenceVersion(),
                    "Local topic cache did not reach v" + targetVersion + " within " + podSwitchWaitMs + "ms"
                )
            );
            return;
        }
        scheduler.schedule(
            () -> pollUntilVersion(event, targetVersion, deadlineMs),
            podPollIntervalMs,
            TimeUnit.MILLISECONDS
        );
    }

    private boolean cacheReached(String topicFqn, int targetVersion) {
        Optional<Resource.EntityResource<VaradhiTopic>> entry = topicCache.get(topicFqn);
        return entry.isPresent() && entry.get().getVersion() >= targetVersion;
    }

    /**
     * The event itself doesn't carry the target region (only the parent FQN + new
     * topic version). The {@link BrokerWarmer} contract therefore receives the local
     * cache's current notion of "where is this topic going" — but at PREPARE time the
     * local cache may not yet have been updated. For now we pass {@code null} which
     * {@link BrokerWarmer#NO_OP} ignores; real implementations are expected to be
     * region-aware via their own context (e.g. local {@code MemberInfo.region}).
     *
     * <p>TODO: extend {@link FailoverStageEvent} with a {@code targetRegion} hint so
     * warmers do not need a side-channel.
     */
    private String resolveTargetRegion(FailoverStageEvent event) {
        return null;
    }
}
