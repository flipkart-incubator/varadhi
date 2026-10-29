package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.core.cluster.MessageExchange;
import com.flipkart.varadhi.core.cluster.controller.FailoverApi;
import com.flipkart.varadhi.core.cluster.controller.TransitionAckApi;
import com.flipkart.varadhi.core.cluster.controller.TransitionPublisher;
import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.core.cluster.transition.TransitionBusAddress;
import com.flipkart.varadhi.core.cluster.transition.TransitionEventBroadcaster;
import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.TopicState;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.VaradhiTopicName;
import com.flipkart.varadhi.entities.cluster.transition.TransitionAck;
import com.flipkart.varadhi.entities.cluster.transition.TransitionEvent;
import com.flipkart.varadhi.entities.cluster.transition.TransitionStage;
import com.flipkart.varadhi.entities.cluster.transition.TransitionType;
import com.flipkart.varadhi.entities.cluster.transition.failover.ProduceFailoverTransition;
import com.flipkart.varadhi.entities.cluster.transition.failover.TopicFailoverOperation;
import com.flipkart.varadhi.entities.cluster.transition.failover.TopicFailoverRequest;
import com.flipkart.varadhi.spi.db.RegionStore;
import com.flipkart.varadhi.spi.db.TopicStore;
import com.flipkart.varadhi.spi.db.TransitionStore;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Controller-local topic-transition service: failover intent CRUD, broadcast, and pod acks.
 */
@Slf4j
public class TransitionService implements TransitionPublisher, TransitionAckApi, FailoverApi {

    private final MessageExchange messageExchange;
    private final OperationMgr operationMgr;
    private final TransitionStore transitionStore;
    private final TopicStore topicStore;
    private final RegionStore regionStore;
    private final RegionName deployedRegion;

    public TransitionService(
        MessageExchange messageExchange,
        OperationMgr operationMgr,
        TransitionStore transitionStore,
        TopicStore topicStore,
        RegionStore regionStore,
        RegionName deployedRegion
    ) {
        this.messageExchange = messageExchange;
        this.operationMgr = operationMgr;
        this.transitionStore = transitionStore;
        this.topicStore = topicStore;
        this.regionStore = regionStore;
        this.deployedRegion = deployedRegion;
        if (operationMgr != null) {
            operationMgr.setTopicFailoverTerminalFailureHandler(this::cleanupFailedTopicFailover);
        }
    }

    @Override
    public CompletableFuture<Void> broadcastEvent(TransitionEvent event) {
        try {
            TransitionEventBroadcaster.publish(messageExchange, event);
            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Void> ack(TransitionAck ack) {
        recordFailoverAck(ack);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<TopicFailoverOperation> createTopicFailover(
        String topicFqn,
        TopicFailoverRequest request,
        String requestedBy
    ) {
        return CompletableFuture.supplyAsync(() -> {
            VaradhiTopic topic = topicStore.get(topicFqn);
            RegionName source = request.sourceRegion() != null ? request.sourceRegion() : deployedRegion;
            RegionName target = request.targetRegion();
            validateFailoverRegions(topic, source, target);
            if (transitionStore.exists(topicFqn)) {
                throw new InvalidOperationForResourceException(
                    "An active failover already exists for topic " + topicFqn + "."
                );
            }
            TopicFailoverOperation op = TopicFailoverOperation.of(topicFqn, requestedBy);
            operationMgr.createTopicFailoverOp(op);
            try {
                transitionStore.create(
                    ProduceFailoverTransition.forFailover(
                        op.getId(),
                        topicFqn,
                        source,
                        target,
                        request.waitForReplicationLagToClear()
                    )
                );
            } catch (RuntimeException e) {
                op.markFail(e.getMessage());
                operationMgr.persistTopicFailoverOp(op);
                throw e;
            }
            log.info("Created topic failover op {} for {} ({}->{})", op.getId(), topicFqn, source, target);
            return op;
        });
    }

    @Override
    public CompletableFuture<ProduceFailoverTransition> getTopicFailover(String topicFqn) {
        return CompletableFuture.supplyAsync(() -> requireActive(topicFqn));
    }

    @Override
    public CompletableFuture<ProduceFailoverTransition> abortTopicFailover(String topicFqn, String requestedBy) {
        return CompletableFuture.supplyAsync(() -> {
            ProduceFailoverTransition transition = requireActive(topicFqn);
            if (!transition.getCurrentStage().isAbortable()) {
                throw new InvalidOperationForResourceException(
                    "Failover for " + topicFqn + " is not abortable in stage " + transition.getCurrentStage() + "."
                );
            }
            log.info(
                "Abort requested for topic failover op {} on {} (requestedBy={})",
                transition.getOperationId(),
                topicFqn,
                requestedBy
            );
            transition.requestAbort();
            transitionStore.update(transition);
            return transition;
        });
    }

    @Override
    public CompletableFuture<List<ProduceFailoverTransition>> getActiveFailovers() {
        return CompletableFuture.supplyAsync(transitionStore::listActive);
    }

    public void recordFailoverAck(TransitionAck ack) {
        log.debug(
            "Failover ack op={} host={} stage={} ok={}",
            ack.opId(),
            ack.hostname(),
            ack.stage(),
            ack.isSuccess()
        );
    }

    public List<TopicFailoverOperation> getPendingTopicFailoverOps() {
        return operationMgr.getPendingTopicFailoverOps();
    }

    public void retryTopicFailover(TopicFailoverOperation operation) {
        log.warn("Cannot resume topic failover op {} — orchestrator not wired on this build", operation.getId());
    }

    private ProduceFailoverTransition requireActive(String topicFqn) {
        if (!transitionStore.exists(topicFqn)) {
            throw new ResourceNotFoundException("No active failover for topic " + topicFqn + ".");
        }
        return transitionStore.get(topicFqn);
    }

    private void cleanupFailedTopicFailover(TopicFailoverOperation op) {
        String topicFqn = op.getTopicFqn();
        if (!transitionStore.exists(topicFqn)) {
            return;
        }
        ProduceFailoverTransition transition = transitionStore.get(topicFqn);
        log.warn("Cleaning up failed topic failover op {} for {} (error={})", op.getId(), topicFqn, op.getErrorMsg());
        messageExchange.publish(
            TransitionBusAddress.ROUTE_TOPIC_TRANSITION,
            TransitionBusAddress.STAGE_BROADCAST_API,
            ClusterMessage.of(
                TransitionEvent.of(
                    transition.getOperationId(),
                    VaradhiTopicName.parse(topicFqn),
                    TransitionType.TOPIC_FAILOVER,
                    TransitionStage.ABORTED,
                    false,
                    0L,
                    null
                )
            )
        );
        transitionStore.delete(topicFqn);
    }

    private void validateFailoverRegions(VaradhiTopic topic, RegionName source, RegionName target) {
        if (source == null || target == null) {
            throw new IllegalArgumentException("sourceRegion and targetRegion are required for failover.");
        }
        if (source.equals(target)) {
            throw new IllegalArgumentException("sourceRegion and targetRegion must differ.");
        }
        requireRegisteredRegion(source);
        requireRegisteredRegion(target);
        if (topic.getProduceConfig(source).isEmpty()) {
            throw new IllegalArgumentException(
                "Topic " + topic.getName() + " is not configured for sourceRegion " + source.value() + "."
            );
        }
        if (topic.getProduceConfig(target).isEmpty()) {
            throw new IllegalArgumentException(
                "Topic " + topic.getName() + " is not configured for targetRegion " + target.value() + "."
            );
        }
        if (topic.getSegmentedStorageTopic() == null || topic.getSegmentedStorageTopic().getStorageTopics() == null
            || topic.getSegmentedStorageTopic().getStorageTopics().length == 0) {
            throw new IllegalArgumentException("Topic " + topic.getName() + " has no storage topic.");
        }
        TopicState sourceState = topic.getProduceConfig(source).orElseThrow().getState();
        if (sourceState != TopicState.Producing) {
            throw new IllegalArgumentException(
                "sourceRegion " + source.value() + " is not producing (state=" + sourceState + ")."
            );
        }
    }

    private void requireRegisteredRegion(RegionName region) {
        if (!regionStore.exists(region.value())) {
            throw new IllegalArgumentException("Region " + region.value() + " is not registered.");
        }
    }
}
