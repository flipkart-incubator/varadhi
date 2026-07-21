package com.flipkart.varadhi.controller.impl.failover;

import com.flipkart.varadhi.controller.OpExecutor;
import com.flipkart.varadhi.controller.OperationMgr;
import com.flipkart.varadhi.core.cluster.ComponentKind;
import com.flipkart.varadhi.core.cluster.MemberInfo;
import com.flipkart.varadhi.core.cluster.MessageExchange;
import com.flipkart.varadhi.core.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.core.cluster.failover.TransitionBusAddress;
import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.RegionConfig;
import com.flipkart.varadhi.entities.TopicRegionConfigs;
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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ForkJoinPool;
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
        // Use thenComposeAsync to break any Vert.x event-loop thread continuation that may be
        // inherited from stage-barrier completion callbacks (which are triggered from the event-loop
        // sendHandler). Running blocking ZK ops or calling serverHosts().join() on the event-loop
        // thread would deadlock because Vert.x Future completion is itself dispatched on that loop.
        if (stage == TransitionStage.PENDING || stage == TransitionStage.PREPARE || stage == TransitionStage.SWITCH) {
            chain = chain.thenComposeAsync(v -> switchStage(op), ForkJoinPool.commonPool());
        }
        if (stage != TransitionStage.COMPLETED && stage != TransitionStage.ABORTED) {
            chain = chain.thenComposeAsync(v -> drain(op), ForkJoinPool.commonPool());
            chain = chain.thenComposeAsync(v -> complete(op), ForkJoinPool.commonPool());
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
        RegionName producing = TopicRegionConfigs.findProducingRegion(topic).orElse(null);
        boolean needsSwitch = producing == null || !Objects.equals(op.getTargetRegion(), producing);
        boolean needsFence = topic.getTopicState().isProduceAllowed();
        if (needsSwitch || needsFence) {
            VaradhiTopic next = topic;
            if (needsSwitch) {
                next = TopicRegionConfigs.withRegionConfigs(
                    topic,
                    switchProduceAllowed(
                        topic.getRegionConfigs(),
                        Objects.requireNonNullElse(producing, op.getSourceRegion()),
                        op.getTargetRegion()
                    )
                );
            }
            if (needsFence) {
                next = next.withTopicState(TopicState.Fenced);
            }
            topicStore.update(next);
            topic = topicStore.get(op.getTopicFqn());
            log.info(
                "Failover op {}: switched producing region to {} and fenced produce for {}, topic now v{}",
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
<<<<<<< HEAD
=======
            stage.isVersionGated(),
>>>>>>> 7fa07080 (fix in failover testing)
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

    private static Map<String, RegionConfig> switchProduceAllowed(
        Map<String, RegionConfig> configs,
        RegionName source,
        RegionName target
    ) {
        Map<String, RegionConfig> updated = new HashMap<>(configs);
        RegionConfig sourceConfig = updated.get(source.value());
        RegionConfig targetConfig = updated.get(target.value());
        updated.put(
            source.value(),
            new RegionConfig(false, sourceConfig != null ? sourceConfig.getFailOverRegion() : null)
        );
        updated.put(
            target.value(),
            new RegionConfig(true, targetConfig != null ? targetConfig.getFailOverRegion() : null)
        );
        return updated;
    }
}
