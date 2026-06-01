package com.flipkart.varadhi.controller.failover;

import com.flipkart.varadhi.controller.OpExecutor;
import com.flipkart.varadhi.controller.OperationMgr;
import com.flipkart.varadhi.core.cluster.MemberInfo;
import com.flipkart.varadhi.core.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.entities.SegmentedStorageTopic;
import com.flipkart.varadhi.entities.TopicState;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.cluster.OrderedOperation;
import com.flipkart.varadhi.entities.cluster.failover.FailoverStage;
import com.flipkart.varadhi.entities.cluster.failover.FailoverStageEvent;
import com.flipkart.varadhi.entities.cluster.failover.PodAckSnapshot;
import com.flipkart.varadhi.entities.cluster.failover.StageSnapshot;
import com.flipkart.varadhi.entities.cluster.failover.TopicFailoverOperation;
import com.flipkart.varadhi.spi.db.OpStore;
import com.flipkart.varadhi.spi.db.TopicFailoverTransactions;
import com.flipkart.varadhi.spi.db.TopicStore;
import com.flipkart.varadhi.spi.services.ReplicationLagPoller;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Driver for a single {@link TopicFailoverOperation}. Implements the stage machine
 * described in the LLD:
 *
 * <pre>
 *   PENDING -> PREPARE -> SWITCH -> DRAIN -> COMPLETED
 *                       \         (controller-only)
 *                        +-----------------------> ABORTED (only before SWITCH succeeds)
 * </pre>
 *
 * <p>Each stage either (a) requires a per-pod ack barrier (PREPARE, SWITCH) or
 * (b) is purely controller-side bookkeeping (PENDING, DRAIN, COMPLETED, ABORTED).
 * The single Topic mutation lives in {@link #runSwitch}; it is committed atomically
 * with the Op-state update through {@link TopicFailoverTransactions#commitSwitch}.
 */
@Slf4j
public final class TopicFailoverOpExecutor implements OpExecutor<OrderedOperation> {

    private final OperationMgr operationMgr;
    private final OpStore opStore;
    private final TopicStore topicStore;
    private final TopicFailoverTransactions failoverTxns;
    private final StageAwaiterRegistry awaiterRegistry;
    private final FailoverBroadcaster broadcaster;
    private final VaradhiClusterManager clusterManager;
    private final ReplicationLagPoller lagPoller;
    private final FailoverConfig config;
    private final ScheduledExecutorService drainScheduler;

    public TopicFailoverOpExecutor(
        OperationMgr operationMgr,
        OpStore opStore,
        TopicStore topicStore,
        TopicFailoverTransactions failoverTxns,
        StageAwaiterRegistry awaiterRegistry,
        FailoverBroadcaster broadcaster,
        VaradhiClusterManager clusterManager,
        ReplicationLagPoller lagPoller,
        FailoverConfig config,
        ScheduledExecutorService drainScheduler
    ) {
        this.operationMgr = operationMgr;
        this.opStore = opStore;
        this.topicStore = topicStore;
        this.failoverTxns = failoverTxns;
        this.awaiterRegistry = awaiterRegistry;
        this.broadcaster = broadcaster;
        this.clusterManager = clusterManager;
        this.lagPoller = lagPoller;
        this.config = config;
        this.drainScheduler = drainScheduler;
    }

    @Override
    public CompletableFuture<Void> execute(OrderedOperation operation) {
        TopicFailoverOperation op = (TopicFailoverOperation) operation;
        log.info("Executing topic failover op {}", op);

        return runPrepare(op).thenCompose(v -> runSwitch(op))
                             .thenCompose(v -> runDrain(op))
                             .thenCompose(v -> cleanup(op, /*success*/ true, null))
                             .exceptionallyCompose(t -> {
                                 log.error("Topic failover op {} failed: {}", op.getId(), t.toString());
                                 return cleanup(op, /*success*/ false, t.getMessage());
                             });
    }

    /* -------------------- PREPARE -------------------- */

    private CompletableFuture<Void> runPrepare(TopicFailoverOperation op) {
        StageSnapshot snap = op.advanceStage(FailoverStage.PREPARE);
        opStore.updateTopicFailoverOp(op);

        Set<String> hosts = liveHosts();
        FailoverStageEvent event = FailoverStageEvent.forPrepare(
            op.getId(),
            op.getData().getTopicFqn(),
            "topic",
            op.getFenceVersion()
        );

        CompletableFuture<List<PodAckSnapshot>> barrier = awaiterRegistry.expect(
            op.getId(),
            FailoverStage.PREPARE,
            op.getFenceVersion(),
            hosts,
            config.getStageAckTimeoutMs(),
            config.getStageAckResendAfterMs(),
            missing -> broadcaster.pushToHosts(event, missing)
        );

        broadcaster.broadcast(event);

        return barrier.handle((acks, t) -> {
            awaiterRegistry.remove(op.getId(), FailoverStage.PREPARE);
            op.completeStage(snap.complete(t == null, t == null ? null : t.getMessage(), acks == null ? List.of() : acks));
            opStore.updateTopicFailoverOp(op);
            if (t != null) {
                throw new RuntimeException("PREPARE failed", t);
            }
            return null;
        });
    }

    /* -------------------- SWITCH -------------------- */

    private CompletableFuture<Void> runSwitch(TopicFailoverOperation op) {
        TopicFailoverOperation.OpData d = op.getData();
        StageSnapshot snap = op.advanceStage(FailoverStage.SWITCH);

        VaradhiTopic topic = topicStore.get(d.getTopicFqn());
        SegmentedStorageTopic source = topic.getProduceTopicForRegion(d.getSourceRegion());
        SegmentedStorageTopic target = topic.getProduceTopicForRegion(d.getTargetRegion());
        if (source == null) {
            throw new IllegalStateException(
                "Topic " + d.getTopicFqn() + " has no internal topic for sourceRegion=" + d.getSourceRegion()
            );
        }
        if (target == null) {
            throw new IllegalStateException(
                "Topic " + d.getTopicFqn() + " has no internal topic for targetRegion=" + d.getTargetRegion()
            );
        }
        source.setTopicState(TopicState.Blocked);
        target.setTopicState(TopicState.Producing);

        failoverTxns.commitSwitch(op, topic);
        int newTopicVersion = topic.getVersion();

        Set<String> hosts = liveHosts();
        FailoverStageEvent event = FailoverStageEvent.forSwitch(
            op.getId(),
            d.getTopicFqn(),
            "topic",
            op.getFenceVersion(),
            newTopicVersion
        );

        CompletableFuture<List<PodAckSnapshot>> barrier = awaiterRegistry.expect(
            op.getId(),
            FailoverStage.SWITCH,
            op.getFenceVersion(),
            hosts,
            config.getStageAckTimeoutMs(),
            config.getStageAckResendAfterMs(),
            missing -> broadcaster.pushToHosts(event, missing)
        );

        broadcaster.broadcast(event);

        return barrier.handle((acks, t) -> {
            awaiterRegistry.remove(op.getId(), FailoverStage.SWITCH);
            op.completeStage(snap.complete(t == null, t == null ? null : t.getMessage(), acks == null ? List.of() : acks));
            opStore.updateTopicFailoverOp(op);
            if (t != null) {
                throw new RuntimeException("SWITCH failed", t);
            }
            return null;
        });
    }

    /* -------------------- DRAIN (controller-only) -------------------- */

    private CompletableFuture<Void> runDrain(TopicFailoverOperation op) {
        if (!op.getData().isWaitForReplicationLagToClear()) {
            log.info("Op {} requested no DRAIN; skipping", op.getId());
            return CompletableFuture.completedFuture(null);
        }
        StageSnapshot snap = op.advanceStage(FailoverStage.DRAIN);
        opStore.updateTopicFailoverOp(op);

        CompletableFuture<Void> done = new CompletableFuture<>();
        long deadline = System.currentTimeMillis() + config.getDrainTimeoutMs();
        scheduleNextDrainTick(op, deadline, snap, done);
        return done;
    }

    private void scheduleNextDrainTick(
        TopicFailoverOperation op,
        long deadlineMs,
        StageSnapshot snap,
        CompletableFuture<Void> done
    ) {
        if (System.currentTimeMillis() > deadlineMs) {
            op.completeStage(snap.complete(false, "DRAIN timed out", List.of()));
            opStore.updateTopicFailoverOp(op);
            done.completeExceptionally(new RuntimeException("DRAIN timed out for op " + op.getId()));
            return;
        }
        drainScheduler.schedule(() -> {
            lagPoller.pollLagMs(op.getData().getTopicFqn(), op.getData().getSourceRegion()).whenComplete((lag, err) -> {
                if (err != null) {
                    log.warn("DRAIN lag poll failed for op {}: {}", op.getId(), err.toString());
                    scheduleNextDrainTick(op, deadlineMs, snap, done);
                    return;
                }
                if (lag != null && lag == 0L) {
                    op.completeStage(snap.complete(true, null, List.of()));
                    opStore.updateTopicFailoverOp(op);
                    done.complete(null);
                } else {
                    scheduleNextDrainTick(op, deadlineMs, snap, done);
                }
            });
        }, config.getDrainPollIntervalMs(), TimeUnit.MILLISECONDS);
    }

    /* -------------------- COMPLETED / ABORTED -------------------- */

    private CompletableFuture<Void> cleanup(TopicFailoverOperation op, boolean success, String error) {
        try {
            if (success) {
                op.markCompleted();
                VaradhiTopic topic = topicStore.get(op.getData().getTopicFqn());
                SegmentedStorageTopic source = topic.getProduceTopicForRegion(op.getData().getSourceRegion());
                if (source != null) {
                    source.setTopicState(TopicState.Replicating);
                }
                failoverTxns.commitSuccess(op, topic, op.getData().getTopicFqn());
            } else {
                op.markFail(error == null ? "failover failed" : error);
                failoverTxns.commitFailure(op, op.getData().getTopicFqn());
            }
        } catch (Exception e) {
            // L1 pipeline + TopicState will self-heal on the next read; log and move on.
            log.error("Cleanup multi-txn failed for op {}: {}", op.getId(), e.toString(), e);
        }
        // Note: OperationMgr.OpTask will see op.isDone() == true and dequeue.
        // operationMgr field is kept on this class so future extensions (e.g. emit
        // telemetry events back through OperationMgr) can hook in without changing
        // the constructor signature.
        if (operationMgr != null) {
            log.debug("Topic failover op {} cleanup complete; final stage={}", op.getId(), op.getCurrentStage());
        }
        return CompletableFuture.completedFuture(null);
    }

    /* -------------------- Helpers -------------------- */

    private Set<String> liveHosts() {
        try {
            List<MemberInfo> members = clusterManager.getAllMembers().toCompletionStage().toCompletableFuture().get();
            return members.stream().map(MemberInfo::hostname).collect(Collectors.toCollection(HashSet::new));
        } catch (Exception e) {
            log.error("Failed to resolve live cluster hosts: {}", e.toString());
            return Set.of();
        }
    }
}
