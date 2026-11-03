package com.flipkart.varadhi.controller.impl.failover;

import com.flipkart.varadhi.common.utils.ThrowableUtils;
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
import com.flipkart.varadhi.entities.cluster.transition.StageSnapshot;
import com.flipkart.varadhi.entities.cluster.transition.TransitionType;
import com.flipkart.varadhi.spi.db.TopicStore;
import com.flipkart.varadhi.spi.db.TransitionStore;
import com.flipkart.varadhi.spi.services.StorageTopicService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Drives a single topic failover: {@code PREPARE → FENCE → MIGRATE → COMPLETED}.
 */
@Slf4j
public class TopicFailoverOpExecutor implements OpExecutor<OrderedOperation>, AutoCloseable {

    private static final long LAG_POLL_INTERVAL_MS = 2_000L;

    private final OperationMgr operationMgr;
    private final TransitionStore transitionStore;
    private final TopicStore topicStore;
    private final StorageTopicService storageTopicService;
    private final MessageExchange messageExchange;
    private final StageAwaiter stageAwaiter;
    private final VaradhiClusterManager clusterManager;
    private final TopicFailoverConfig config;

    private final Consumer<TopicFailoverOperation> failureHandler;
    private final ExecutorService failoverStageExecutor;
    private final ExecutorService replicationLagExecutor;

    public TopicFailoverOpExecutor(
        OperationMgr operationMgr,
        TransitionStore transitionStore,
        TopicStore topicStore,
        StorageTopicService storageTopicService,
        MessageExchange messageExchange,
        StageAwaiter stageAwaiter,
        VaradhiClusterManager clusterManager,
        TopicFailoverConfig config,
        Consumer<TopicFailoverOperation> failureHandler
    ) {
        this.operationMgr = operationMgr;
        this.transitionStore = transitionStore;
        this.topicStore = topicStore;
        this.storageTopicService = storageTopicService;
        this.messageExchange = messageExchange;
        this.stageAwaiter = stageAwaiter;
        this.clusterManager = clusterManager;
        this.config = config;
        this.failureHandler = failureHandler;
        this.failoverStageExecutor = Executors.newFixedThreadPool(
            5,
            new ThreadFactoryBuilder().setNameFormat("topic-failover-stage-%d").setDaemon(true).build()
        );
        this.replicationLagExecutor = Executors.newFixedThreadPool(
            2,
            new ThreadFactoryBuilder().setNameFormat("topic-failover-lag-%d").setDaemon(true).build()
        );
    }

    @Override
    public void close() {
        failoverStageExecutor.shutdownNow();
        replicationLagExecutor.shutdownNow();
    }

    @Override
    public CompletableFuture<Void> execute(OrderedOperation operation) {
        TopicFailoverOperation op = (TopicFailoverOperation)operation;
        return CompletableFuture.supplyAsync(() -> executeInternal(op), failoverStageExecutor)
                                .thenCompose(Function.identity());
    }

    private CompletableFuture<Void> executeInternal(TopicFailoverOperation op) {
        try {
            String fqn = op.getTopicFqn().toFqn();
            if (!transitionStore.exists(fqn)) {
                throw new FailoverAbortedException("no active transition for topic " + fqn);
            }
            ProduceFailoverTransition transition = transitionStore.get(fqn);
            ensureNotAborted(transition);
            log.info("Executing topic failover op {} resuming from stage {}", op.getId(), transition.getCurrentStage());
            return resumeFrom(op, transition).thenRun(() -> finishSuccess(op)).exceptionally(t -> {
                handleFailure(op, ThrowableUtils.rootMessage(t));
                if (t instanceof RuntimeException re) {
                    throw re;
                }
                throw new CompletionException(t);
            });
        } catch (Exception e) {
            handleFailure(op, ThrowableUtils.rootMessage(e));
            return CompletableFuture.failedFuture(e);
        }
    }

    private void handleFailure(TopicFailoverOperation op, String errorMsg) {
        op.markFail(errorMsg);
        if (failureHandler != null) {
            failureHandler.accept(op);
        }
    }

    private CompletableFuture<Void> resumeFrom(TopicFailoverOperation op, ProduceFailoverTransition transition) {
        TransitionStage stage = transition.getCurrentStage();
        CompletableFuture<Map<String, StageSnapshot.HostAck>> chain = CompletableFuture.completedFuture(Map.of());

        if (stage == TransitionStage.PENDING || stage == TransitionStage.PREPARE) {
            chain = chain.thenComposeAsync(acks -> prepare(op), failoverStageExecutor);
        }
        if (stage == TransitionStage.PENDING || stage == TransitionStage.PREPARE || stage == TransitionStage.FENCE) {
            chain = chain.thenComposeAsync(acks -> fence(op, acks), failoverStageExecutor);
        }
        if (stage == TransitionStage.PENDING || stage == TransitionStage.PREPARE || stage == TransitionStage.FENCE
            || stage == TransitionStage.MIGRATE) {
            chain = chain.thenComposeAsync(acks -> migrateStage(op, acks), failoverStageExecutor);
        }
        if (stage != TransitionStage.COMPLETED && stage != TransitionStage.ABORTED) {
            chain = chain.thenComposeAsync(acks -> complete(op, acks), failoverStageExecutor);
        }
        return chain.thenRun(() -> {});
    }

    private CompletableFuture<Map<String, StageSnapshot.HostAck>> prepare(TopicFailoverOperation op) {
        VaradhiTopic topic = topicStore.get(op.getTopicFqn().toFqn());
        long currentVersion = topic.getVersion();
        ProduceFailoverTransition transition = transitionStore.get(op.getTopicFqn().toFqn());
        advanceMaster(op, transition, TransitionStage.PREPARE, currentVersion, Map.of());
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
    private CompletableFuture<Map<String, StageSnapshot.HostAck>> fence(
        TopicFailoverOperation op,
        Map<String, StageSnapshot.HostAck> prepareHostAcks
    ) {
        ProduceFailoverTransition transition = transitionStore.get(op.getTopicFqn().toFqn());
        ensureNotAborted(transition);
        VaradhiTopic topic = topicStore.get(op.getTopicFqn().toFqn());
        RegionName source = transition.getSourceRegion();
        RegionName target = transition.getTargetRegion();
        if (!isState(topic, source, TopicState.Fenced) || !isState(topic, target, TopicState.Fenced)) {
            ProduceConfig sourcePrev = topic.getProduceConfig(source).orElse(ProduceConfig.producing());
            ProduceConfig targetPrev = topic.getProduceConfig(target).orElse(ProduceConfig.blocked());
            VaradhiTopic fencedTopic = topic.with(
                source,
                new ProduceConfig(TopicState.Fenced, sourcePrev.getProduceIdx(), target)
            ).with(target, new ProduceConfig(TopicState.Fenced, targetPrev.getProduceIdx(), null));
            topicStore.update(fencedTopic);
            topic = topicStore.get(op.getTopicFqn().toFqn());
        }
        long fencedVersion = topic.getVersion();
        advanceMaster(op, transition, TransitionStage.FENCE, fencedVersion, prepareHostAcks);
        TransitionEvent fenceEvent = stageEvent(transition, TransitionStage.FENCE, fencedVersion, null);
        return runStageBarrier(op, TransitionStage.FENCE, fenceEvent, config.switchTimeoutMs()).thenComposeAsync(
            fenceAcks -> {
                ProduceFailoverTransition latestTransition = transitionStore.get(op.getTopicFqn().toFqn());
                ensureNotAborted(latestTransition);
                if (!latestTransition.isWaitForReplicationLagToClear()) {
                    return CompletableFuture.completedFuture(fenceAcks);
                }
                return CompletableFuture.runAsync(() -> awaitLagCleared(latestTransition), replicationLagExecutor)
                                        .thenApply(v -> fenceAcks);
            },
            failoverStageExecutor
        );
    }

    private void awaitLagCleared(ProduceFailoverTransition transition) {
        VaradhiTopic topic = topicStore.get(transition.getTopicFqn());
        SegmentedStorageTopic segmented = topic.getSegmentedStorageTopic();
        if (segmented == null || segmented.getStorageTopics() == null || segmented.getStorageTopics().length == 0) {
            throw new FailoverAbortedException("FENCE: topic " + transition.getTopicFqn() + " has no storage topic");
        }
        int produceIdx = topic.getProduceConfig(transition.getSourceRegion())
                              .map(ProduceConfig::getProduceIdx)
                              .orElse(0);
        StorageTopic storage = segmented.getTopic(produceIdx);
        long drainTimeoutMs = transition.getLagDrainTimeoutMs() != null ?
            transition.getLagDrainTimeoutMs() :
            config.drainTimeoutMs();
        long deadline = System.currentTimeMillis() + drainTimeoutMs;
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
                if ("FAILOVER".equals(transition.getOnLagTimeout())) {
                    log.warn(
                        "Failover op {}: lag did not clear within {}ms; proceeding with route switch.",
                        transition.getOperationId(),
                        drainTimeoutMs
                    );
                    return;
                }
                throw new FailoverAbortedException(
                    "replication lag did not clear within %dms (last lag=%d) for %s".formatted(
                        drainTimeoutMs,
                        lastLag,
                        transition.getTopicFqn()
                    )
                );
            }
            try {
                Thread.sleep(Math.min(LAG_POLL_INTERVAL_MS, remaining));
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
    private CompletableFuture<Map<String, StageSnapshot.HostAck>> migrateStage(
        TopicFailoverOperation op,
        Map<String, StageSnapshot.HostAck> fenceHostAcks
    ) {
        ProduceFailoverTransition transition = transitionStore.get(op.getTopicFqn().toFqn());
        ensureNotAborted(transition);
        VaradhiTopic topic = topicStore.get(op.getTopicFqn().toFqn());
        RegionName source = transition.getSourceRegion();
        RegionName target = transition.getTargetRegion();
        boolean routeCommitted = isState(topic, target, TopicState.Producing) && isState(
            topic,
            source,
            TopicState.Blocked
        );
        long switchedVersion = transition.getTopicVersionToAwait();
        if (transition.getCurrentStage() != TransitionStage.MIGRATE) {
            switchedVersion = topic.getVersion() + 1L;
            advanceMaster(op, transition, TransitionStage.MIGRATE, switchedVersion, fenceHostAcks);
        }
        if (!routeCommitted) {
            if (topic.getVersion() >= switchedVersion) {
                throw new FailoverAbortedException("topic version changed before route commit for " + op.getTopicFqn());
            }
            ProduceConfig targetPrev = topic.getProduceConfig(target).orElse(ProduceConfig.blocked());
            ProduceConfig sourcePrev = topic.getProduceConfig(source).orElse(ProduceConfig.producing());
            VaradhiTopic switchedTopic = topic.with(
                target,
                new ProduceConfig(TopicState.Producing, targetPrev.getProduceIdx(), null)
            ).with(source, new ProduceConfig(TopicState.Blocked, sourcePrev.getProduceIdx(), null));
            topicStore.update(switchedTopic);
            topic = topicStore.get(op.getTopicFqn().toFqn());
            if (topic.getVersion() != switchedVersion) {
                throw new FailoverAbortedException(
                    "topic version did not converge after route commit for " + op.getTopicFqn()
                );
            }
            log.info(
                "Failover op {}: switched produce for {} ({} -> {}), topic now v{}",
                op.getId(),
                op.getTopicFqn(),
                source.value(),
                target.value(),
                topic.getVersion()
            );
        }

        operationMgr.updateTopicFailoverOp(op);
        TransitionEvent event = stageEvent(transition, TransitionStage.MIGRATE, switchedVersion, null);
        return runStageBarrier(op, TransitionStage.MIGRATE, event, config.switchTimeoutMs());
    }

    private static void ensureNotAborted(ProduceFailoverTransition transition) {
        if (transition.isAbortRequested()) {
            log.info(
                "Failover op {} for {} aborted during stage {}",
                transition.getOperationId(),
                transition.getTopicFqn(),
                transition.getCurrentStage()
            );
            throw new FailoverAbortedException("topic failover aborted for " + transition.getTopicFqn());
        }
    }

    private CompletableFuture<Map<String, StageSnapshot.HostAck>> complete(
        TopicFailoverOperation op,
        Map<String, StageSnapshot.HostAck> migrateHostAcks
    ) {
        VaradhiTopic topic = topicStore.get(op.getTopicFqn().toFqn());
        ProduceFailoverTransition transition = transitionStore.get(op.getTopicFqn().toFqn());
        RegionName target = transition.getTargetRegion();
        RegionName source = transition.getSourceRegion();
        if (!isState(topic, target, TopicState.Producing) || !isState(topic, source, TopicState.Blocked)) {
            ProduceConfig targetPrev = topic.getProduceConfig(target).orElse(ProduceConfig.blocked());
            ProduceConfig sourcePrev = topic.getProduceConfig(source).orElse(ProduceConfig.producing());
            VaradhiTopic completedTopic = topic.with(
                target,
                new ProduceConfig(TopicState.Producing, targetPrev.getProduceIdx(), null)
            ).with(source, new ProduceConfig(TopicState.Blocked, sourcePrev.getProduceIdx(), null));
            topicStore.update(completedTopic);
            topic = completedTopic;
        }
        advanceMaster(op, transition, TransitionStage.COMPLETED, 0L, migrateHostAcks);
        broadcast(stageEvent(transition, TransitionStage.COMPLETED, 0L, null));
        return CompletableFuture.completedFuture(Map.of());
    }

    /**
     * Advances the durable transition. No-op when already at {@code stage} (resume).
     */
    private void advanceMaster(
        TopicFailoverOperation op,
        ProduceFailoverTransition transition,
        TransitionStage stage,
        long topicVersionToAwait,
        Map<String, StageSnapshot.HostAck> hostAcks
    ) {
        if (transition.getCurrentStage() == stage) {
            return;
        }
        log.info(
            "Failover op {} for {}: transitioning from stage {} to {}",
            op.getId(),
            op.getTopicFqn(),
            transition.getCurrentStage(),
            stage
        );
        transition.advanceTo(stage, topicVersionToAwait, hostAcks, null);
        transitionStore.update(transition);
        operationMgr.updateTopicFailoverOp(op);
    }

    private CompletableFuture<Map<String, StageSnapshot.HostAck>> runStageBarrier(
        TopicFailoverOperation op,
        TransitionStage stage,
        TransitionEvent event,
        long timeoutMs
    ) {
        Set<String> hosts = serverHosts();
        CompletableFuture<Void> barrier = stageAwaiter.expect(op.getId(), stage, hosts, timeoutMs);
        broadcast(event);
        return barrier.thenApply(v -> stageAwaiter.getHostAcks(op.getId()))
                      .whenComplete((acks, t) -> stageAwaiter.clear(op.getId()));
    }

    private void finishSuccess(TopicFailoverOperation op) {
        op.markCompleted();
        operationMgr.updateTopicFailoverOp(op);
        transitionStore.delete(op.getTopicFqn().toFqn());
        log.info("Topic failover op {} for {} completed.", op.getId(), op.getTopicFqn().toFqn());
    }

    private TransitionEvent stageEvent(
        ProduceFailoverTransition transition,
        TransitionStage stage,
        long topicVersionToAwait,
        TransitionEvent.Target target
    ) {
        return TransitionEvent.of(
            transition.getOperationId(),
            VaradhiTopicName.parse(transition.getTopicFqn()),
            TransitionType.TOPIC_FAILOVER,
            stage,
            stage.needsTopicVersionSync(),
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

    private static boolean isState(VaradhiTopic topic, RegionName region, TopicState state) {
        return topic.getProduceConfig(region).map(ProduceConfig::getState).orElse(null) == state;
    }
}
