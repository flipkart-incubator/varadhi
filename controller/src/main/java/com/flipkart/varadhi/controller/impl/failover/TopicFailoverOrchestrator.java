package com.flipkart.varadhi.controller.impl.failover;

import com.flipkart.varadhi.controller.OperationMgr;
import com.flipkart.varadhi.core.cluster.MessageExchange;
import com.flipkart.varadhi.core.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.entities.ProduceConfig;
import com.flipkart.varadhi.entities.TopicState;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.cluster.transition.TransitionAck;
import com.flipkart.varadhi.entities.cluster.transition.failover.ProduceFailoverTransition;
import com.flipkart.varadhi.entities.cluster.transition.failover.TopicFailoverOperation;
import com.flipkart.varadhi.spi.db.TopicStore;
import com.flipkart.varadhi.spi.db.TransitionStore;
import com.flipkart.varadhi.spi.services.StorageTopicService;

/**
 * Owns controller-side execution and recovery of topic failover operations.
 */
public final class TopicFailoverOrchestrator {

    private static final long TOPIC_FAILOVER_STAGE_AWAIT_MS = 60_000L;
    private static final long TOPIC_FAILOVER_DRAIN_TIMEOUT_MS = 300_000L;

    private final OperationMgr operationMgr;
    private final TransitionStore transitionStore;
    private final TopicStore topicStore;
    private final StageAwaiter stageAwaiter;
    private final TopicFailoverOpExecutor executor;

    public TopicFailoverOrchestrator(
        OperationMgr operationMgr,
        TransitionStore transitionStore,
        TopicStore topicStore,
        StorageTopicService storageTopicService,
        MessageExchange messageExchange,
        VaradhiClusterManager clusterManager
    ) {
        this.operationMgr = operationMgr;
        this.transitionStore = transitionStore;
        this.topicStore = topicStore;
        this.stageAwaiter = new StageAwaiter();
        this.executor = new TopicFailoverOpExecutor(
            operationMgr,
            transitionStore,
            topicStore,
            storageTopicService,
            messageExchange,
            stageAwaiter,
            clusterManager,
            new TopicFailoverConfig(
                TOPIC_FAILOVER_STAGE_AWAIT_MS,
                TOPIC_FAILOVER_STAGE_AWAIT_MS,
                TOPIC_FAILOVER_DRAIN_TIMEOUT_MS
            )
        );
    }

    public void enqueue(TopicFailoverOperation operation) {
        operationMgr.enqueueTopicFailover(operation, executor);
    }

    public void abort(ProduceFailoverTransition transition, String reason) {
        transition.setAbortRequested(true);
        transitionStore.update(transition);
        stageAwaiter.abort(transition.getOperationId(), reason);
        rollbackFenceIfNeeded(transition);
    }

    public void abortBarrier(String operationId, String reason) {
        stageAwaiter.abort(operationId, reason);
    }

    public void recordAck(TransitionAck ack) {
        stageAwaiter.recordAck(ack);
    }

    private void rollbackFenceIfNeeded(ProduceFailoverTransition transition) {
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
        if (transition.getCurrentStage() != com.flipkart.varadhi.entities.cluster.transition.TransitionStage.DRAIN
            && !(sourceFenced && targetFenced)) {
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
}
