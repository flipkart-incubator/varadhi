package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.controller.impl.failover.StageAwaiter;
import com.flipkart.varadhi.controller.impl.failover.TopicFailoverConfig;
import com.flipkart.varadhi.controller.impl.failover.TopicFailoverOpExecutor;
import com.flipkart.varadhi.core.cluster.MessageExchange;
import com.flipkart.varadhi.core.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.core.cluster.controller.FailoverApi;
import com.flipkart.varadhi.core.cluster.controller.TransitionAckApi;
import com.flipkart.varadhi.core.cluster.controller.TransitionPublisher;
import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.core.cluster.transition.TransitionBusAddress;
import com.flipkart.varadhi.core.cluster.transition.TransitionEventBroadcaster;
import com.flipkart.varadhi.entities.ProduceConfig;
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
import com.flipkart.varadhi.spi.db.TopicStore;
import com.flipkart.varadhi.spi.db.TransitionStore;
import com.flipkart.varadhi.spi.services.StorageTopicService;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Controller-local topic-transition service: failover intent CRUD, stage execution, broadcast, and pod acks.
 */
@Slf4j
public class TransitionService implements TransitionPublisher, TransitionAckApi, FailoverApi, AutoCloseable {

    private static final long TOPIC_FAILOVER_STAGE_AWAIT_MS = 60_000L;
    private static final long TOPIC_FAILOVER_DRAIN_TIMEOUT_MS = 300_000L;

    private final MessageExchange messageExchange;
    private final OperationMgr operationMgr;
    private final TransitionStore transitionStore;
    private final TopicStore topicStore;
    private final RegionName deployedRegion;

    private final StageAwaiter stageAwaiter;
    private final TopicFailoverOpExecutor executor;

    public TransitionService(
        MessageExchange messageExchange,
        OperationMgr operationMgr,
        TransitionStore transitionStore,
        TopicStore topicStore,
        StorageTopicService storageTopicService,
        VaradhiClusterManager clusterManager,
        RegionName deployedRegion
    ) {
        this.messageExchange = messageExchange;
        this.operationMgr = operationMgr;
        this.transitionStore = transitionStore;
        this.topicStore = topicStore;
        this.deployedRegion = deployedRegion;

        TopicFailoverConfig failoverConfig = new TopicFailoverConfig(
            TOPIC_FAILOVER_STAGE_AWAIT_MS,
            TOPIC_FAILOVER_STAGE_AWAIT_MS,
            TOPIC_FAILOVER_DRAIN_TIMEOUT_MS
        );

        if (topicStore != null && storageTopicService != null && clusterManager != null) {
            this.stageAwaiter = new StageAwaiter();
            this.executor = new TopicFailoverOpExecutor(
                operationMgr,
                transitionStore,
                topicStore,
                storageTopicService,
                messageExchange,
                stageAwaiter,
                clusterManager,
                failoverConfig,
                this::cleanupFailedTopicFailover
            );
        } else {
            this.stageAwaiter = null;
            this.executor = null;
        }
    }

    @Override
    public void close() {
        if (executor != null) {
            executor.close();
        }
    }

    public void enqueue(TopicFailoverOperation operation) {
        if (executor != null) {
            operationMgr.enqueueTopicFailover(operation, executor);
        }
    }

    public void abort(ProduceFailoverTransition transition, String reason) {
        transition.setAbortRequested(true);
        transitionStore.update(transition);
        if (stageAwaiter != null) {
            stageAwaiter.abort(transition.getOperationId(), reason);
        }
        rollbackFenceIfNeeded(transition);
    }

    public void recordAck(TransitionAck ack) {
        if (stageAwaiter != null) {
            stageAwaiter.recordAck(ack);
        }
    }

    private void rollbackFenceIfNeeded(ProduceFailoverTransition transition) {
        if (topicStore == null) {
            return;
        }
        VaradhiTopic topic = topicStore.get(transition.getTopicFqn());
        boolean sourceProducing = topic.getProduceConfig(transition.getSourceRegion())
                                       .map(config -> config.getState() == TopicState.Producing)
                                       .orElse(false);
        boolean targetBlocked = topic.getProduceConfig(transition.getTargetRegion())
                                     .map(config -> config.getState() == TopicState.Blocked)
                                     .orElse(false);
        boolean sourceFenced = topic.getProduceConfig(transition.getSourceRegion())
                                    .map(config -> config.getState() == TopicState.Fenced)
                                    .orElse(false);
        boolean targetFenced = topic.getProduceConfig(transition.getTargetRegion())
                                    .map(config -> config.getState() == TopicState.Fenced)
                                    .orElse(false);
        if (transition.getCurrentStage() != TransitionStage.FENCE && !(sourceFenced && targetFenced)) {
            return;
        }
        if (sourceProducing && targetBlocked) {
            return;
        }
        ProduceConfig source = topic.getProduceConfig(transition.getSourceRegion()).orElse(ProduceConfig.producing());
        ProduceConfig target = topic.getProduceConfig(transition.getTargetRegion()).orElse(ProduceConfig.blocked());
        topicStore.update(
            topic.with(
                transition.getSourceRegion(),
                new ProduceConfig(TopicState.Producing, source.getProduceIdx(), null)
            ).with(transition.getTargetRegion(), new ProduceConfig(TopicState.Blocked, target.getProduceIdx(), null))
        );
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
        recordAck(ack);
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
            TopicFailoverOperation op = TopicFailoverOperation.of(topicFqn, requestedBy);
            operationMgr.createTopicFailoverOp(op);
            try {
                if (request.lagDrainTimeoutMs() != null && request.lagDrainTimeoutMs() <= 0) {
                    throw new IllegalArgumentException("lagDrainTimeoutMs must be positive");
                }
                ProduceFailoverTransition transition = ProduceFailoverTransition.forFailover(
                    op.getId(),
                    fqn,
                    source,
                    target,
                    request.waitForReplicationLagToClear(),
                    request.lagDrainTimeoutMs(),
                    normalizeLagTimeoutPolicy(request.onLagTimeout())
                );
                transitionStore.create(transition);
                enqueue(op);
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
            abort(transition, "aborted by " + requestedBy);
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
        enqueue(operation);
    }

    private ProduceFailoverTransition requireActive(String topicFqn) {
        return transitionStore.get(topicFqn);
    }

    private static String normalizeLagTimeoutPolicy(String policy) {
        if (policy == null || policy.isBlank()) {
            return "ABORT";
        }
        String normalized = policy.trim().toUpperCase(java.util.Locale.ROOT);
        if (!normalized.equals("ABORT") && !normalized.equals("FAILOVER")) {
            throw new IllegalArgumentException("onLagTimeout must be ABORT or FAILOVER");
        }
        return normalized;
    }

    private void cleanupFailedTopicFailover(TopicFailoverOperation op) {
        String topicFqn = op.getTopicFqn().toFqn();
        if (!transitionStore.exists(topicFqn)) {
            return;
        }
        ProduceFailoverTransition transition = transitionStore.get(topicFqn);
        log.warn("Cleaning up failed topic failover op {} for {} (error={})", op.getId(), topicFqn, op.getErrorMsg());
        abort(transition, op.getErrorMsg());
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
