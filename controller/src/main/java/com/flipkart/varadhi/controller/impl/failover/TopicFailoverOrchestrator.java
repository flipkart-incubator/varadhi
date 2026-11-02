package com.flipkart.varadhi.controller.impl.failover;

import com.flipkart.varadhi.controller.OperationMgr;
import com.flipkart.varadhi.core.cluster.MessageExchange;
import com.flipkart.varadhi.core.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.entities.cluster.transition.failover.ProduceFailoverTransition;
import com.flipkart.varadhi.entities.cluster.transition.failover.TopicFailoverOperation;
import com.flipkart.varadhi.spi.db.TopicStore;
import com.flipkart.varadhi.spi.db.TransitionStore;
import com.flipkart.varadhi.spi.services.StorageTopicService;

/**
 * Owns controller-side execution and recovery of topic failover operations.
 */
public final class TopicFailoverOrchestrator {

    private final OperationMgr operationMgr;
    private final TransitionStore transitionStore;
    private final StageAwaiter stageAwaiter;
    private final TopicFailoverOpExecutor executor;

    public TopicFailoverOrchestrator(
        OperationMgr operationMgr,
        TransitionStore transitionStore,
        TopicStore topicStore,
        StorageTopicService storageTopicService,
        MessageExchange messageExchange,
        VaradhiClusterManager clusterManager,
        long stageAwaitMs,
        long drainTimeoutMs
    ) {
        this.operationMgr = operationMgr;
        this.transitionStore = transitionStore;
        this.stageAwaiter = new StageAwaiter();
        this.executor = new TopicFailoverOpExecutor(
            operationMgr,
            transitionStore,
            topicStore,
            storageTopicService,
            messageExchange,
            stageAwaiter,
            clusterManager,
            new TopicFailoverConfig(stageAwaitMs, stageAwaitMs, drainTimeoutMs)
        );
    }

    public void enqueue(TopicFailoverOperation operation) {
        operationMgr.enqueueTopicFailover(operation, executor);
    }

    public void abort(ProduceFailoverTransition transition, String reason) {
        transition.setAbortRequested(true);
        transitionStore.update(transition);
        stageAwaiter.abort(transition.getOperationId(), reason);
    }

    public void abortBarrier(String operationId, String reason) {
        stageAwaiter.abort(operationId, reason);
    }
}
