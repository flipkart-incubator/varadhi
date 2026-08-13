package com.flipkart.varadhi.controller.impl.failover;

import com.flipkart.varadhi.controller.OpExecutor;
import com.flipkart.varadhi.controller.OperationMgr;
import com.flipkart.varadhi.core.cluster.ComponentKind;
import com.flipkart.varadhi.core.cluster.MemberInfo;
import com.flipkart.varadhi.core.cluster.MessageExchange;
import com.flipkart.varadhi.core.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.core.cluster.failover.TransitionBusAddress;
import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.entities.ProduceConfig;
import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.SegmentedStorageTopic;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.TopicState;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.VaradhiTopicName;
import com.flipkart.varadhi.entities.cluster.OrderedOperation;
import com.flipkart.varadhi.entities.cluster.TopicFailoverOperation;
import com.flipkart.varadhi.entities.cluster.failover.TransitionEvent;
import com.flipkart.varadhi.entities.cluster.failover.TransitionMaster;
import com.flipkart.varadhi.entities.cluster.failover.TransitionStage;
import com.flipkart.varadhi.entities.cluster.failover.TransitionType;
import com.flipkart.varadhi.spi.db.TopicStore;
import com.flipkart.varadhi.spi.db.TransitionStore;
import com.flipkart.varadhi.spi.services.StorageTopicService;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

/**
 * Drives a single topic failover: {@code PREPARE → FENCE → MIGRATE → COMPLETED}.
 * FENCE stops produce on the ingress region; MIGRATE drains replication then commits
 * {@code Producing + failOverRegion=target} on that same region (internal retarget).
 */
@Slf4j
public class TopicFailoverOpExecutor implements OpExecutor<OrderedOperation> {

    private static final long LAG_POLL_INTERVAL_MS = 2_000L;

    private final OperationMgr operationMgr;
    private final TransitionStore transitionStore;
    private final TopicStore topicStore;
    private final StorageTopicService storageTopicService;
    private final MessageExchange messageExchange;
    private final StageAwaiter stageAwaiter;
    private final VaradhiClusterManager clusterManager;
    private final TopicFailoverConfig config;
    private final RegionName deployedRegion;

    public TopicFailoverOpExecutor(
        OperationMgr operationMgr,
        TransitionStore transitionStore,
        TopicStore topicStore,
        StorageTopicService storageTopicService,
        MessageExchange messageExchange,
        StageAwaiter stageAwaiter,
        VaradhiClusterManager clusterManager,
        TopicFailoverConfig config,
        RegionName deployedRegion
    ) {
        this.operationMgr = operationMgr;
        this.transitionStore = transitionStore;
        this.topicStore = topicStore;
        this.storageTopicService = storageTopicService;
        this.messageExchange = messageExchange;
        this.stageAwaiter = stageAwaiter;
        this.clusterManager = clusterManager;
        this.config = config;
        this.deployedRegion = deployedRegion;
    }

    @Override
    public CompletableFuture<Void> execute(OrderedOperation operation) {
        TopicFailoverOperation op = (TopicFailoverOperation)operation;
        String fqn = op.getTopicFqn();
        try {
            if (!transitionStore.exists(fqn)) {
                throw new FailoverAbortedException("no active transition for topic " + fqn);
            }
            TransitionMaster transition = transitionStore.get(fqn);
            log.info("Executing topic failover op {} resuming from stage {}", op.getId(), transition.getCurrentStage());
            return resumeFrom(op, transition).thenRun(() -> finishSuccess(op));
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private CompletableFuture<Void> resumeFrom(TopicFailoverOperation op, TransitionMaster transition) {
        TransitionStage stage = transition.getCurrentStage();
        CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
        if (stage == TransitionStage.PENDING || stage == TransitionStage.PREPARE) {
            chain = chain.thenCompose(v -> prepare(op));
        }
        // thenComposeAsync: keep blocking ZK / lag / member lookup off the Vert.x event loop.
        if (stage == TransitionStage.PENDING || stage == TransitionStage.PREPARE || stage == TransitionStage.FENCE) {
            chain = chain.thenComposeAsync(v -> fence(op), ForkJoinPool.commonPool());
        }
        if (stage == TransitionStage.PENDING || stage == TransitionStage.PREPARE || stage == TransitionStage.FENCE
            || stage == TransitionStage.MIGRATE) {
            chain = chain.thenComposeAsync(v -> migrate(op), ForkJoinPool.commonPool());
        }
        if (stage != TransitionStage.COMPLETED && stage != TransitionStage.ABORTED) {
            chain = chain.thenComposeAsync(v -> complete(op), ForkJoinPool.commonPool());
        }
        return chain;
    }

    private CompletableFuture<Void> prepare(TopicFailoverOperation op) {
        VaradhiTopic topic = topicStore.get(op.getTopicFqn());
        long currentVersion = topic.getVersion();
        TransitionMaster transition = transitionStore.get(op.getTopicFqn());
        advanceMaster(op, transition, TransitionStage.PREPARE, currentVersion);
        TransitionEvent event = stageEvent(
            op,
            TransitionStage.PREPARE,
            currentVersion,
            new TransitionEvent.Target.Region(op.getTargetRegion())
        );
        return runStageBarrier(op, TransitionStage.PREPARE, event, config.stageAwaitMs(), currentVersion);
    }

    /**
     * Fence ingress only ({@code state=Fenced}); do not set {@code failOverRegion} yet.
     */
    private CompletableFuture<Void> fence(TopicFailoverOperation op) {
        VaradhiTopic topic = topicStore.get(op.getTopicFqn());
        RegionName source = ingressRegion(op, topic);
        if (!isState(topic, source, TopicState.Fenced)) {
            ProduceConfig prev = topic.getProduceConfig(source).orElse(ProduceConfig.producing());
            topicStore.update(
                topic.with(source, new ProduceConfig(TopicState.Fenced, prev.getProduceIdx(), prev.getFailOverRegion().orElse(null)))
            );
            topic = topicStore.get(op.getTopicFqn());
            log.info(
                "Failover op {}: fenced ingress {} for {} topic now v{}",
                op.getId(),
                source.value(),
                op.getTopicFqn(),
                topic.getVersion()
            );
        }
        long fencedVersion = topic.getVersion();
        TransitionMaster transition = transitionStore.get(op.getTopicFqn());
        advanceMaster(op, transition, TransitionStage.FENCE, fencedVersion);
        TransitionEvent event = stageEvent(op, TransitionStage.FENCE, fencedVersion, null);
        return runStageBarrier(op, TransitionStage.FENCE, event, config.stageAwaitMs(), fencedVersion)
            .thenCompose(v -> holdAfterFence(op));
    }

    private CompletableFuture<Void> holdAfterFence(TopicFailoverOperation op) {
        long holdMs = config.fenceHoldMs();
        if (holdMs <= 0) {
            return CompletableFuture.completedFuture(null);
        }
        log.info("Failover op {}: fenceHoldMs={} before MIGRATE", op.getId(), holdMs);
        return CompletableFuture.runAsync(
            () -> { },
            CompletableFuture.delayedExecutor(holdMs, java.util.concurrent.TimeUnit.MILLISECONDS)
        );
    }

    /**
     * Lag poll (optional), then commit ingress {@code Producing + failOverRegion=target}, then pod barrier.
     */
    private CompletableFuture<Void> migrate(TopicFailoverOperation op) {
        TransitionMaster transition = transitionStore.get(op.getTopicFqn());
        advanceMaster(op, transition, TransitionStage.MIGRATE, 0L);

        return CompletableFuture.runAsync(() -> {
            if (op.isWaitForReplicationLagToClear()) {
                awaitLagCleared(op);
            }
            commitRoute(op);
        }, ForkJoinPool.commonPool()).thenCompose(v -> {
            VaradhiTopic topic = topicStore.get(op.getTopicFqn());
            long committedVersion = topic.getVersion();
            TransitionMaster latest = transitionStore.get(op.getTopicFqn());
            // Refresh await version on master for resume / pod barrier.
            if (latest.getTopicVersionToAwait() != committedVersion) {
                latest.advanceTo(TransitionStage.MIGRATE, committedVersion);
                transitionStore.update(latest);
            }
            TransitionEvent event = stageEvent(op, TransitionStage.MIGRATE, committedVersion, null);
            return runStageBarrier(op, TransitionStage.MIGRATE, event, config.stageAwaitMs(), committedVersion);
        });
    }

    private void commitRoute(TopicFailoverOperation op) {
        VaradhiTopic topic = topicStore.get(op.getTopicFqn());
        RegionName source = ingressRegion(op, topic);
        RegionName target = op.getTargetRegion();
        ProduceConfig prev = topic.getProduceConfig(source).orElse(ProduceConfig.producing());
        boolean alreadyCommitted = prev.getState() == TopicState.Producing
            && prev.getFailOverRegion().map(target::equals).orElse(false);
        if (alreadyCommitted) {
            return;
        }
        topicStore.update(
            topic.with(source, new ProduceConfig(TopicState.Producing, prev.getProduceIdx(), target))
        );
        log.info(
            "Failover op {}: committed failOverRegion={} on {} for {}",
            op.getId(),
            target.value(),
            source.value(),
            op.getTopicFqn()
        );
    }

    private void awaitLagCleared(TopicFailoverOperation op) {
        VaradhiTopic topic = topicStore.get(op.getTopicFqn());
        SegmentedStorageTopic segmented = topic.getSegmentedStorageTopic();
        if (segmented == null || segmented.getStorageTopics() == null || segmented.getStorageTopics().length == 0) {
            throw new FailoverAbortedException("MIGRATE: topic " + op.getTopicFqn() + " has no storage topic");
        }
        int produceIdx = topic.getProduceConfig(op.getSourceRegion()).map(ProduceConfig::getProduceIdx).orElse(0);
        StorageTopic storage = segmented.getTopic(produceIdx);
        long deadline = System.currentTimeMillis() + config.drainTimeoutMs();
        long lastLag;
        while (true) {
            lastLag = storageTopicService.getReplicationLag(storage, op.getSourceRegion(), op.getTargetRegion());
            if (lastLag <= 0) {
                log.info(
                    "Failover op {}: replication lag cleared for {} ({} -> {})",
                    op.getId(),
                    op.getTopicFqn(),
                    op.getSourceRegion().value(),
                    op.getTargetRegion().value()
                );
                return;
            }
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                throw new FailoverAbortedException(
                    "replication lag did not clear within %dms (last lag=%d) for %s".formatted(
                        config.drainTimeoutMs(),
                        lastLag,
                        op.getTopicFqn()
                    )
                );
            }
            try {
                Thread.sleep(Math.min(LAG_POLL_INTERVAL_MS, remaining));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FailoverAbortedException("interrupted waiting for replication lag on " + op.getTopicFqn());
            }
        }
    }

    /** Controller bookkeeping only — pods are terminal after MIGRATE. */
    private CompletableFuture<Void> complete(TopicFailoverOperation op) {
        TransitionMaster transition = transitionStore.get(op.getTopicFqn());
        advanceMaster(op, transition, TransitionStage.COMPLETED, 0L);
        broadcast(stageEvent(op, TransitionStage.COMPLETED, 0L, null));
        return CompletableFuture.completedFuture(null);
    }

    private void advanceMaster(
        TopicFailoverOperation op,
        TransitionMaster transition,
        TransitionStage stage,
        long topicVersionToAwait
    ) {
        if (transition.getCurrentStage() == stage) {
            if (topicVersionToAwait > 0L && transition.getTopicVersionToAwait() != topicVersionToAwait) {
                transition.advanceTo(stage, topicVersionToAwait);
                transitionStore.update(transition);
            }
            return;
        }
        transition.advanceTo(stage, topicVersionToAwait);
        transitionStore.update(transition);
        op.beginStage(stage);
        operationMgr.updateTopicFailoverOp(op);
    }

    private CompletableFuture<Void> runStageBarrier(
        TopicFailoverOperation op,
        TransitionStage stage,
        TransitionEvent event,
        long timeoutMs,
        long requiredTopicVersion
    ) {
        Set<String> hosts = serverHosts();
        CompletableFuture<Void> barrier = stageAwaiter.expect(
            op.getId(),
            stage,
            hosts,
            timeoutMs,
            requiredTopicVersion
        );
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
        TransitionEvent.Target target
    ) {
        return TransitionEvent.of(
            op.getId(),
            VaradhiTopicName.parse(op.getTopicFqn()),
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

    private RegionName ingressRegion(TopicFailoverOperation op, VaradhiTopic topic) {
        // Prefer the region's own entry that is being failed over (source); fall back to deployed.
        return Objects.requireNonNullElse(op.getSourceRegion(), deployedRegion);
    }

    private static boolean isState(VaradhiTopic topic, RegionName region, TopicState state) {
        return topic.getProduceConfig(region).map(ProduceConfig::getState).orElse(null) == state;
    }
}
