package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.core.cluster.MessageExchange;
import com.flipkart.varadhi.core.cluster.controller.FailoverApi;
import com.flipkart.varadhi.core.cluster.controller.TransitionAckApi;
import com.flipkart.varadhi.core.cluster.controller.TransitionPublisher;
import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.core.cluster.transition.TransitionBusAddress;
import com.flipkart.varadhi.core.cluster.transition.TransitionEventBroadcaster;
import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.VaradhiTopicName;
import com.flipkart.varadhi.entities.cluster.transition.TransitionAck;
import com.flipkart.varadhi.entities.cluster.transition.TransitionEvent;
import com.flipkart.varadhi.entities.cluster.transition.TransitionStage;
import com.flipkart.varadhi.entities.cluster.transition.TransitionType;
import com.flipkart.varadhi.entities.cluster.transition.failover.ProduceFailoverTransition;
import com.flipkart.varadhi.entities.cluster.transition.failover.TopicFailoverOperation;
import com.flipkart.varadhi.entities.cluster.transition.failover.TopicFailoverRequest;
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
    private final RegionName deployedRegion;

    public TransitionService(
        MessageExchange messageExchange,
        OperationMgr operationMgr,
        TransitionStore transitionStore,
        RegionName deployedRegion
    ) {
        this.messageExchange = messageExchange;
        this.operationMgr = operationMgr;
        this.transitionStore = transitionStore;
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
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<TopicFailoverOperation> createTopicFailover(
        VaradhiTopicName topicFqn,
        TopicFailoverRequest request,
        String requestedBy
    ) {
        return CompletableFuture.supplyAsync(() -> {
            String fqn = topicFqn.toFqn();
            RegionName source = request.sourceRegion() != null ? request.sourceRegion() : deployedRegion;
            RegionName target = request.targetRegion();
            if (transitionStore.exists(fqn)) {
                throw new InvalidOperationForResourceException(
                    "An active failover already exists for topic " + fqn + "."
                );
            }
            TopicFailoverOperation op = TopicFailoverOperation.of(fqn, requestedBy);
            operationMgr.createTopicFailoverOp(op);
            try {
                transitionStore.create(
                    ProduceFailoverTransition.forFailover(
                        op.getId(),
                        fqn,
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
            log.info("Created topic failover op {} for {} ({}->{})", op.getId(), fqn, source, target);
            return op;
        });
    }

    @Override
    public CompletableFuture<ProduceFailoverTransition> getTopicFailover(VaradhiTopicName topicFqn) {
        return CompletableFuture.supplyAsync(() -> requireActive(topicFqn.toFqn()));
    }

    @Override
    public CompletableFuture<ProduceFailoverTransition> abortTopicFailover(
        VaradhiTopicName topicFqn,
        String requestedBy
    ) {
        return CompletableFuture.supplyAsync(() -> {
            String fqn = topicFqn.toFqn();
            ProduceFailoverTransition transition = requireActive(fqn);
            if (!transition.getCurrentStage().isAbortable()) {
                throw new InvalidOperationForResourceException(
                    "Failover for " + fqn + " is not abortable in stage " + transition.getCurrentStage() + "."
                );
            }
            log.info(
                "Abort requested for topic failover op {} on {} (requestedBy={})",
                transition.getOperationId(),
                fqn,
                requestedBy
            );
            transition.setAbortRequested(true);
            transitionStore.update(transition);
            return transition;
        });
    }

    @Override
    public CompletableFuture<List<ProduceFailoverTransition>> getActiveFailovers() {
        return CompletableFuture.supplyAsync(transitionStore::listActive);
    }

    public List<TopicFailoverOperation> getPendingTopicFailoverOps() {
        return operationMgr.getPendingTopicFailoverOps();
    }

    public void retryTopicFailover(TopicFailoverOperation operation) {
        throw new UnsupportedOperationException(
            "Topic failover retry is not supported for op " + operation.getId()
        );
    }

    private ProduceFailoverTransition requireActive(String topicFqn) {
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
}
