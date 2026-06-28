package com.flipkart.varadhi.controller.impl.failover;

import com.flipkart.varadhi.controller.OpExecutor;
import com.flipkart.varadhi.controller.OperationMgr;
import com.flipkart.varadhi.core.cluster.ComponentKind;
import com.flipkart.varadhi.core.cluster.MemberInfo;
import com.flipkart.varadhi.core.cluster.MessageExchange;
import com.flipkart.varadhi.core.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.core.cluster.failover.TransitionBusAddress;
import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.entities.TopicState;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.VaradhiTopicName;
import com.flipkart.varadhi.entities.cluster.OrderedOperation;
import com.flipkart.varadhi.entities.cluster.TopicFailoverOperation;
import com.flipkart.varadhi.entities.cluster.failover.TransitionEvent;
import com.flipkart.varadhi.entities.cluster.failover.TransitionObject;
import com.flipkart.varadhi.entities.cluster.failover.TransitionStage;
import com.flipkart.varadhi.entities.cluster.failover.TransitionType;
import com.flipkart.varadhi.spi.db.TopicStore;
import com.flipkart.varadhi.spi.db.TransitionStore;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

/**
 * Drives a single topic failover through its stages on the controller.
 */
@Slf4j
public class TopicFailoverOpExecutor implements OpExecutor<OrderedOperation> {

    private final OperationMgr operationMgr;
    private final TransitionStore transitionStore;
    private final TopicStore topicStore;
    private final MessageExchange messageExchange;
    private final StageAwaiter stageAwaiter;
    private final VaradhiClusterManager clusterManager;
    private final TopicFailoverConfig config;

    public TopicFailoverOpExecutor(
        OperationMgr operationMgr,
        TransitionStore transitionStore,
        TopicStore topicStore,
        MessageExchange messageExchange,
        StageAwaiter stageAwaiter,
        VaradhiClusterManager clusterManager,
        TopicFailoverConfig config
    ) {
        this.operationMgr = operationMgr;
        this.transitionStore = transitionStore;
        this.topicStore = topicStore;
        this.messageExchange = messageExchange;
        this.stageAwaiter = stageAwaiter;
        this.clusterManager = clusterManager;
        this.config = config;
    }

    @Override
    public CompletableFuture<Void> execute(OrderedOperation operation) {
        TopicFailoverOperation op = (TopicFailoverOperation)operation;
        String fqn = op.getTopicFqn();
        try {
            if (!transitionStore.exists(fqn)) {
                throw new FailoverAbortedException("no active transition for topic " + fqn);
            }
            TransitionObject transition = transitionStore.get(fqn);
            log.info("Executing topic failover op {} resuming from stage {}", op.getId(), transition.getCurrentStage());
            return resumeFrom(op, transition).thenRun(() -> finishSuccess(op));
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private CompletableFuture<Void> resumeFrom(TopicFailoverOperation op, TransitionObject transition) {
        TransitionStage stage = transition.getCurrentStage();
        CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
        if (stage == TransitionStage.PENDING || stage == TransitionStage.PREPARE) {
            chain = chain.thenCompose(v -> prepare(op));
        }
        if (stage == TransitionStage.PENDING || stage == TransitionStage.PREPARE || stage == TransitionStage.SWITCH) {
            chain = chain.thenCompose(v -> switchStage(op));
        }
        if (stage != TransitionStage.COMPLETED && stage != TransitionStage.ABORTED) {
            chain = chain.thenCompose(v -> drain(op));
            chain = chain.thenCompose(v -> complete(op));
        }
        return chain;
    }

    private CompletableFuture<Void> prepare(TopicFailoverOperation op) {
        VaradhiTopic topic = topicStore.get(op.getTopicFqn());
        long currentVersion = topic.getVersion();
        TransitionObject transition = transitionStore.get(op.getTopicFqn());
        if (transition.getCurrentStage() != TransitionStage.PREPARE) {
            transition.advanceTo(TransitionStage.PREPARE, currentVersion);
            transitionStore.update(transition);
        }
        TransitionEvent event = stageEvent(op, TransitionStage.PREPARE, currentVersion, op.getTargetRegion().value());
        return runStageBarrier(op, TransitionStage.PREPARE, event, config.prepareTimeoutMs());
    }

    private CompletableFuture<Void> switchStage(TopicFailoverOperation op) {
        VaradhiTopic topic = topicStore.get(op.getTopicFqn());
        boolean needsActiveRegionUpdate = !Objects.equals(op.getTargetRegion(), topic.getActiveRegion());
        boolean needsBlock = topic.getTopicState().isProduceAllowed();
        if (needsActiveRegionUpdate || needsBlock) {
            VaradhiTopic next = needsActiveRegionUpdate ? topic.withActiveRegion(op.getTargetRegion()) : topic;
            if (needsBlock) {
                next = next.withTopicState(TopicState.Blocked);
            }
            topicStore.update(next);
            topic = topicStore.get(op.getTopicFqn());
            log.info(
                "Failover op {}: switched activeRegion to {} and blocked produce for {}, topic now v{}",
                op.getId(),
                op.getTargetRegion().value(),
                op.getTopicFqn(),
                topic.getVersion()
            );
        }
        long switchedVersion = topic.getVersion();

        TransitionObject transition = transitionStore.get(op.getTopicFqn());
        if (transition.getCurrentStage() != TransitionStage.SWITCH) {
            transition.advanceTo(TransitionStage.SWITCH, switchedVersion);
            transitionStore.update(transition);
        }
        TransitionEvent event = stageEvent(op, TransitionStage.SWITCH, switchedVersion, null);
        return runStageBarrier(op, TransitionStage.SWITCH, event, config.switchTimeoutMs());
    }

    private CompletableFuture<Void> drain(TopicFailoverOperation op) {
        TransitionObject transition = transitionStore.get(op.getTopicFqn());
        transition.advanceTo(TransitionStage.DRAIN, 0L);
        transitionStore.update(transition);
        broadcast(stageEvent(op, TransitionStage.DRAIN, 0L, null));
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> complete(TopicFailoverOperation op) {
        VaradhiTopic topic = topicStore.get(op.getTopicFqn());
        if (!topic.getTopicState().isProduceAllowed()) {
            topicStore.update(topic.withTopicState(TopicState.Producing));
        }
        TransitionObject transition = transitionStore.get(op.getTopicFqn());
        transition.advanceTo(TransitionStage.COMPLETED, 0L);
        transitionStore.update(transition);
        broadcast(stageEvent(op, TransitionStage.COMPLETED, 0L, null));
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> runStageBarrier(
        TopicFailoverOperation op,
        TransitionStage stage,
        TransitionEvent event,
        long timeoutMs
    ) {
        Set<String> hosts = serverHosts();
        CompletableFuture<Void> barrier = stageAwaiter.expect(op.getId(), stage, hosts, timeoutMs);
        broadcast(event);
        return barrier.whenComplete((v, t) -> stageAwaiter.clear(op.getId()));
    }

    private void finishSuccess(TopicFailoverOperation op) {
        op.markCompleted();
        transitionStore.delete(op.getTopicFqn());
        operationMgr.updateTopicFailoverOp(op);
        log.info("Topic failover op {} for {} completed.", op.getId(), op.getTopicFqn());
    }

    private TransitionEvent stageEvent(
        TopicFailoverOperation op,
        TransitionStage stage,
        long topicVersionToAwait,
        String target
    ) {
        return TransitionEvent.of(
            op.getId(),
            VaradhiTopicName.parse(op.getTopicFqn()),
            TransitionType.TOPIC_FAILOVER,
            stage,
            topicVersionToAwait,
            target
        );
    }

    private void broadcast(TransitionEvent event) {
        messageExchange.publish(
            TransitionBusAddress.ROUTE_TOPIC_TRANSITION,
            TransitionBusAddress.STAGE_BROADCAST_API,
            ClusterMessage.of(event)
        );
    }

    private Set<String> serverHosts() {
        try {
            return clusterManager.getAllMembers()
                                 .toCompletionStage()
                                 .toCompletableFuture()
                                 .join()
                                 .stream()
                                 .filter(m -> m.hasRole(ComponentKind.Server))
                                 .map(MemberInfo::hostname)
                                 .collect(Collectors.toSet());
        } catch (CompletionException e) {
            throw new FailoverAbortedException("failed to resolve cluster members: " + e.getMessage());
        }
    }
}
