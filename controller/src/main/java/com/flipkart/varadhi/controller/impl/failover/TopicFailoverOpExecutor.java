package com.flipkart.varadhi.controller.impl.failover;

import com.flipkart.varadhi.controller.OpExecutor;
import com.flipkart.varadhi.controller.OperationMgr;
import com.flipkart.varadhi.core.cluster.ComponentKind;
import com.flipkart.varadhi.core.cluster.MemberInfo;
import com.flipkart.varadhi.core.cluster.MessageExchange;
import com.flipkart.varadhi.core.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.core.cluster.transition.TransitionBusAddress;
import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.entities.ProduceConfig;
import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.SegmentedStorageTopic;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.TopicState;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.VaradhiTopicName;
import com.flipkart.varadhi.entities.cluster.OrderedOperation;
import com.flipkart.varadhi.entities.cluster.transition.failover.TopicFailoverOperation;
import com.flipkart.varadhi.entities.cluster.transition.TransitionEvent;
import com.flipkart.varadhi.entities.cluster.transition.failover.ProduceFailoverTransition;
import com.flipkart.varadhi.entities.cluster.transition.TransitionStage;
import com.flipkart.varadhi.entities.cluster.transition.TransitionType;
import com.flipkart.varadhi.spi.db.TopicStore;
import com.flipkart.varadhi.spi.db.TransitionStore;
import com.flipkart.varadhi.spi.services.StorageTopicService;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

/**
 * Drives a single topic failover: {@code PREPARE → DRAIN → SWITCH → COMPLETE}.
 */
@Slf4j
public class TopicFailoverOpExecutor implements OpExecutor<OrderedOperation> {

    private static final long DRAIN_POLL_INTERVAL_MS = 2_000L;

    private final OperationMgr operationMgr;
    private final TransitionStore transitionStore;
    private final TopicStore topicStore;
    private final StorageTopicService storageTopicService;
    private final MessageExchange messageExchange;
    private final StageAwaiter stageAwaiter;
    private final VaradhiClusterManager clusterManager;
    private final TopicFailoverConfig config;

    public TopicFailoverOpExecutor(
        OperationMgr operationMgr,
        TransitionStore transitionStore,
        TopicStore topicStore,
        StorageTopicService storageTopicService,
        MessageExchange messageExchange,
        StageAwaiter stageAwaiter,
        VaradhiClusterManager clusterManager,
        TopicFailoverConfig config
    ) {
        this.operationMgr = operationMgr;
        this.transitionStore = transitionStore;
        this.topicStore = topicStore;
        this.storageTopicService = storageTopicService;
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
            ProduceFailoverTransition transition = transitionStore.get(fqn);
            ensureNotAborted(transition);
            log.info("Executing topic failover op {} resuming from stage {}", op.getId(), transition.getCurrentStage());
            return resumeFrom(op, transition).thenRun(() -> finishSuccess(op));
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private CompletableFuture<Void> resumeFrom(TopicFailoverOperation op, ProduceFailoverTransition transition) {
        TransitionStage stage = transition.getCurrentStage();
        CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
        if (stage == TransitionStage.PENDING || stage == TransitionStage.PREPARE) {
            chain = chain.thenCompose(v -> prepare(op));
        }
        // thenComposeAsync: break Vert.x event-loop continuations from stage-barrier callbacks so
        // blocking ZK / lag polls / serverHosts().join() never run on the event loop.
        if (stage == TransitionStage.PENDING || stage == TransitionStage.PREPARE || stage == TransitionStage.DRAIN) {
            chain = chain.thenComposeAsync(v -> drain(op), ForkJoinPool.commonPool());
        }
        if (stage == TransitionStage.PENDING || stage == TransitionStage.PREPARE || stage == TransitionStage.DRAIN
            || stage == TransitionStage.SWITCH) {
            chain = chain.thenComposeAsync(v -> switchStage(op), ForkJoinPool.commonPool());
        }
        if (stage != TransitionStage.COMPLETED && stage != TransitionStage.ABORTED) {
            chain = chain.thenComposeAsync(v -> complete(op), ForkJoinPool.commonPool());
        }
        return chain;
    }

    private CompletableFuture<Void> prepare(TopicFailoverOperation op) {
        VaradhiTopic topic = topicStore.get(op.getTopicFqn());
        long currentVersion = topic.getVersion();
        ProduceFailoverTransition transition = transitionStore.get(op.getTopicFqn());
        advanceMaster(op, transition, TransitionStage.PREPARE, currentVersion);
        TransitionEvent event = stageEvent(
            transition,
            TransitionStage.PREPARE,
            currentVersion,
            new TransitionEvent.Target.Region(transition.getTargetRegion())
        );
        return runStageBarrier(op, TransitionStage.PREPARE, event, config.prepareTimeoutMs());
    }

    /**
     * Fences produce before waiting for source→target replication lag to clear. This prevents new
     * messages from arriving after the lag check and before the route commit.
     */
    private CompletableFuture<Void> drain(TopicFailoverOperation op) {
        VaradhiTopic topic = topicStore.get(op.getTopicFqn());
        ProduceFailoverTransition transition = transitionStore.get(op.getTopicFqn());
        ensureNotAborted(transition);
        RegionName source = transition.getSourceRegion();
        RegionName target = transition.getTargetRegion();
        if (!isState(topic, source, TopicState.Fenced) || !isState(topic, target, TopicState.Fenced)) {
            ProduceConfig sourcePrev = topic.getProduceConfig(source).orElse(ProduceConfig.producing());
            ProduceConfig targetPrev = topic.getProduceConfig(target).orElse(ProduceConfig.blocked());
            topicStore.update(
                topic.with(source, new ProduceConfig(TopicState.Fenced, sourcePrev.getProduceIdx(), target))
                     .with(target, new ProduceConfig(TopicState.Fenced, targetPrev.getProduceIdx(), null))
            );
            topic = topicStore.get(op.getTopicFqn());
        }
        long fencedVersion = topic.getVersion();
        advanceMaster(op, transition, TransitionStage.DRAIN, fencedVersion);
        TransitionEvent fenceEvent = stageEvent(transition, TransitionStage.DRAIN, fencedVersion, null);
        return runStageBarrier(op, TransitionStage.DRAIN, fenceEvent, config.switchTimeoutMs())
                .thenCompose(v -> {
                    ensureNotAborted(transitionStore.get(op.getTopicFqn()));
                    if (!transition.isWaitForReplicationLagToClear()) {
                        return CompletableFuture.completedFuture(null);
                    }
                    return CompletableFuture.runAsync(() -> awaitLagCleared(transition), ForkJoinPool.commonPool());
                });
    }

    private void awaitLagCleared(ProduceFailoverTransition transition) {
        VaradhiTopic topic = topicStore.get(transition.getTopicFqn());
        SegmentedStorageTopic segmented = topic.getSegmentedStorageTopic();
        if (segmented == null || segmented.getStorageTopics() == null || segmented.getStorageTopics().length == 0) {
            throw new FailoverAbortedException("DRAIN: topic " + transition.getTopicFqn() + " has no storage topic");
        }
        int produceIdx = topic.getProduceConfig(transition.getSourceRegion())
                              .map(ProduceConfig::getProduceIdx)
                              .orElse(0);
        StorageTopic storage = segmented.getTopic(produceIdx);
        long deadline = System.currentTimeMillis() + config.drainTimeoutMs();
        long lastLag = -1;
        while (true) {
            ensureNotAborted(transitionStore.get(transition.getTopicFqn()));
            lastLag = storageTopicService.getReplicationLag(
                storage,
                transition.getSourceRegion(),
                transition.getTargetRegion()
            );
            if (lastLag <= 0) {
                log.info(
                    "Failover op {}: replication lag cleared for {} ({} -> {})",
                    transition.getOperationId(),
                    transition.getTopicFqn(),
                    transition.getSourceRegion().value(),
                    transition.getTargetRegion().value()
                );
                return;
            }
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                throw new FailoverAbortedException(
                    "replication lag did not clear within %dms (last lag=%d) for %s".formatted(
                        config.drainTimeoutMs(),
                        lastLag,
                        transition.getTopicFqn()
                    )
                );
            }
            try {
                Thread.sleep(Math.min(DRAIN_POLL_INTERVAL_MS, remaining));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FailoverAbortedException(
                    "interrupted waiting for replication lag on " + transition.getTopicFqn()
                );
            }
        }
    }

    /**
     * Commits the route switch after the source has been fenced and replication lag has drained.
     */
    private CompletableFuture<Void> switchStage(TopicFailoverOperation op) {
        VaradhiTopic topic = topicStore.get(op.getTopicFqn());
        ProduceFailoverTransition transition = transitionStore.get(op.getTopicFqn());
        ensureNotAborted(transition);
        RegionName source = transition.getSourceRegion();
        RegionName target = transition.getTargetRegion();
        if (!isState(topic, target, TopicState.Producing) || !isState(topic, source, TopicState.Blocked)) {
            ProduceConfig targetPrev = topic.getProduceConfig(target).orElse(ProduceConfig.blocked());
            ProduceConfig sourcePrev = topic.getProduceConfig(source).orElse(ProduceConfig.producing());
            topicStore.update(
                topic.with(target, new ProduceConfig(TopicState.Producing, targetPrev.getProduceIdx(), null))
                     .with(source, new ProduceConfig(TopicState.Blocked, sourcePrev.getProduceIdx(), null))
            );
            topic = topicStore.get(op.getTopicFqn());
            log.info(
                "Failover op {}: switched produce for {} ({} -> {}), topic now v{}",
                op.getId(),
                op.getTopicFqn(),
                source.value(),
                target.value(),
                topic.getVersion()
            );
        }
        long switchedVersion = topic.getVersion();

        advanceMaster(op, transition, TransitionStage.SWITCH, switchedVersion);
        TransitionEvent event = stageEvent(transition, TransitionStage.SWITCH, switchedVersion, null);
        return runStageBarrier(op, TransitionStage.SWITCH, event, config.switchTimeoutMs());
    }

    private static void ensureNotAborted(ProduceFailoverTransition transition) {
        if (transition.isAbortRequested()) {
            throw new FailoverAbortedException(
                "topic failover aborted for " + transition.getTopicFqn()
            );
        }
    }

    private CompletableFuture<Void> complete(TopicFailoverOperation op) {
        VaradhiTopic topic = topicStore.get(op.getTopicFqn());
        ProduceFailoverTransition transition = transitionStore.get(op.getTopicFqn());
        RegionName target = transition.getTargetRegion();
        RegionName source = transition.getSourceRegion();
        if (!isState(topic, target, TopicState.Producing) || !isState(topic, source, TopicState.Blocked)) {
            ProduceConfig targetPrev = topic.getProduceConfig(target).orElse(ProduceConfig.blocked());
            ProduceConfig sourcePrev = topic.getProduceConfig(source).orElse(ProduceConfig.producing());
            topicStore.update(
                topic.with(target, new ProduceConfig(TopicState.Producing, targetPrev.getProduceIdx(), null))
                     .with(source, new ProduceConfig(TopicState.Blocked, sourcePrev.getProduceIdx(), null))
            );
        }
        advanceMaster(op, transition, TransitionStage.COMPLETED, 0L);
        broadcast(stageEvent(transition, TransitionStage.COMPLETED, 0L, null));
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Advances the durable transition. No-op when already at {@code stage} (resume).
     */
    private void advanceMaster(
        TopicFailoverOperation op,
        ProduceFailoverTransition transition,
        TransitionStage stage,
        long topicVersionToAwait
    ) {
        if (transition.getCurrentStage() == stage) {
            return;
        }
        transition.advanceTo(stage, topicVersionToAwait, Map.of(), null);
        transitionStore.update(transition);
        operationMgr.updateTopicFailoverOp(op);
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
        operationMgr.updateTopicFailoverOp(op);
        transitionStore.delete(op.getTopicFqn());
        log.info("Topic failover op {} for {} completed.", op.getId(), op.getTopicFqn());
    }

    private TransitionEvent stageEvent(
        ProduceFailoverTransition transition,
        TransitionStage stage,
        long topicVersionToAwait,
        TransitionEvent.Target target
    ) {
        com.flipkart.varadhi.entities.cluster.transition.TransitionStage wireStage = wireStage(stage);
        return TransitionEvent.of(
            transition.getOperationId(),
            VaradhiTopicName.parse(transition.getTopicFqn()),
            TransitionType.TOPIC_FAILOVER,
            wireStage,
            wireStage.needsTopicVersionSync(),
            topicVersionToAwait,
            target
        );
    }

    private static com.flipkart.varadhi.entities.cluster.transition.TransitionStage wireStage(TransitionStage stage) {
        return switch (stage) {
            case DRAIN -> com.flipkart.varadhi.entities.cluster.transition.TransitionStage.FENCE;
            case SWITCH -> com.flipkart.varadhi.entities.cluster.transition.TransitionStage.MIGRATE;
            default -> com.flipkart.varadhi.entities.cluster.transition.TransitionStage.valueOf(stage.name());
        };
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

    private static boolean isState(VaradhiTopic topic, RegionName region, TopicState state) {
        return topic.getProduceConfig(region).map(ProduceConfig::getState).orElse(null) == state;
    }
}
