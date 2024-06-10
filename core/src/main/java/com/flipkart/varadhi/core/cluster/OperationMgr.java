package com.flipkart.varadhi.core.cluster;

import com.flipkart.varadhi.entities.SubscriptionUnitShard;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.spi.db.OpStore;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;


@Slf4j
public class OperationMgr {
    private final OpStore opStore;
    private final ExecutorService executor;
    private final Map<String, Deque<OpTask>> subOps;

    public OperationMgr(OpStore opStore) {
        this.opStore = opStore;
        this.subOps = new ConcurrentHashMap<>();
        //TODO::Add config for number of threads.
        //TODO::ExecutorService should emit the metrics.
        this.executor = Executors.newFixedThreadPool(2, new ThreadFactoryBuilder().setNameFormat("OpMgr-%d").build());
    }

    /**
     * Task Execution
     * - Task embeds a Subscription operation to be executed.
     * - Task are keyed by subscription. A subscription can have multiple tasks waiting be executed. However only one
     * will be executed.
     * - Tasks for different subscriptions can be executed in parallel. Parallelism is controlled via number of threads
     * in the executor service.
     * - Task at the head of the subscription's scheduleTasks queue will be in progress and rest will be waiting.
     * queueSubOp -- adds the operation to the queue. If it is only task, it will be immediately scheduled for execution.
     * handleSubOpUpdate -- Removes the tasks from the queue, if finished (Completed or Errored). If current task
     * finished, schedule next task for execution when available.
     * TODO:: Implementation can be enhanced for below
     * - Remove redundant operations from the queue.
     * - Implement retry logic for failed operation to support auto recovery from temporary failure.
     */

    private void queueSubOp(
            SubscriptionOperation operation, Function<SubscriptionOperation, CompletableFuture<Void>> opHandler
    ) {
        SubscriptionOperation.OpData pendingOp = operation.getData();
        subOps.compute(pendingOp.getSubscriptionId(), (subId, scheduledTasks) -> {
            OpTask pendingTask = OpTask.of(operation, opHandler);
            if (null == scheduledTasks) {
                Deque<OpTask> taskQueue = new ArrayDeque<>();
                taskQueue.addLast(pendingTask);
                executor.submit(pendingTask);
                log.info("Scheduling the SubOp({}) for execution.", pendingOp);
                return taskQueue;
            } else {
                // it means already some operations are scheduled, add this to queue.
                int pending = scheduledTasks.size();
                log.info("Subscription has {} pending operations, queued SubOp({}).", pending, pendingOp);
                // duplicate shouldn't happen unless it is called multiple times e.g. as part of retry.
                boolean alreadyScheduled = false;
                for (OpTask task : scheduledTasks) {
                    if (task.operation.getData().getOperationId().equals(pendingOp.getOperationId())) {
                        log.warn("SubOp({}) is already scheduled. Ignoring duplicate.", pendingOp);
                        alreadyScheduled = true;
                    }
                }
                if (!alreadyScheduled) {
                    scheduledTasks.addLast(pendingTask);
                }
                return scheduledTasks;
            }
        });
    }

    // This will execute the update on a subscription in a sequential order. Sequential execution is needed, to ensure
    // parallel updates to Subscription operation from different shards, do not override each other.
    private void handleSubOpUpdate(
            SubscriptionOperation operation, Function<SubscriptionOperation, CompletableFuture<Void>> updateHandler
    ) {
        SubscriptionOperation.OpData updated = operation.getData();
        subOps.compute(operation.getData().getSubscriptionId(), (subId, scheduledTasks) -> {
            if (null != scheduledTasks && !scheduledTasks.isEmpty()) {

                // process the update using provided handler.
                // Update processing can take time, this will affect a subscription.
                if (null != updateHandler) {
                    //TODO::apply failure.
                    updateHandler.apply(operation);
                }

                SubscriptionOperation.OpData inProgress = scheduledTasks.peekFirst().operation.getData();
                if (!updated.getOperationId().equals(inProgress.getOperationId())) {
                    // This shouldn't happen as only task at the head is scheduled for execution.
                    log.error("Obtained update for waiting SubOp, Updated({}), InProgress({}).", updated, inProgress);
                    return scheduledTasks;
                }

                // Remove completed operation from the pending list and schedule next operation if available.
                if (operation.completed()) {
                    scheduledTasks.removeFirst();
                    log.info("Completed SubOp({}) removed from the queue.", updated);
                    if (scheduledTasks.isEmpty()) {
                        return null;
                    } else {
                        OpTask waiting = scheduledTasks.peekFirst();
                        log.info("Pending SubOp({}) scheduled for execution.", waiting.operation.getData());
                        executor.submit(waiting);
                    }
                }
                return scheduledTasks;
            } else {
                log.error("SubOp {} not found for update.", updated);
                return null;
            }
        });
    }

    public SubscriptionOperation requestSubStart(
            String subscriptionId, String requestedBy,
            Function<SubscriptionOperation, CompletableFuture<Void>> provider
    ) {
        SubscriptionOperation operation = SubscriptionOperation.startOp(subscriptionId, requestedBy);
        opStore.createSubOp(operation);
        queueSubOp(operation, provider);
        return operation;
    }

    public SubscriptionOperation requestSubStop(
            String subscriptionId, String requestedBy,
            Function<SubscriptionOperation, CompletableFuture<Void>> provider
    ) {
        SubscriptionOperation operation = SubscriptionOperation.stopOp(subscriptionId, requestedBy);
        opStore.createSubOp(operation);
        queueSubOp(operation, provider);
        return operation;
    }

    public void updateSubOp(SubscriptionOperation subscriptionOp) {
        handleSubOpUpdate(subscriptionOp, subOp -> {
            // updating DB status in handler, to avoid version conflict.
            SubscriptionOperation subOpLatest = opStore.getSubOp(subscriptionOp.getData().getOperationId());
            subOpLatest.update(subscriptionOp);
            opStore.updateSubOp(subOpLatest);
            return CompletableFuture.completedFuture(null);
        });
    }

    public ShardOperation requestShardStart(
            String subOpId, SubscriptionUnitShard shard, VaradhiSubscription subscription
    ) {
        ShardOperation startOp = ShardOperation.startOp(subOpId, shard, subscription);
        opStore.createShardOp(startOp);
        return startOp;
    }

    public ShardOperation requestShardStop(
            String subOpId, SubscriptionUnitShard shard, VaradhiSubscription subscription
    ) {
        ShardOperation stopOp = ShardOperation.stopOp(subOpId, shard, subscription);
        opStore.createShardOp(stopOp);
        return stopOp;
    }

    public void updateShardOp(ShardOperation.OpData opData) {
        // updating DB status in handler for both Shard and Subscription op, to avoid version conflict.
        SubscriptionOperation subscriptionOp = opStore.getSubOp(opData.getParentOpId());
        handleSubOpUpdate(subscriptionOp, subOp -> doShardOpAndSubscriptionOpUpdate(subOp, opData));
    }

    private CompletableFuture<Void> doShardOpAndSubscriptionOpUpdate(
            SubscriptionOperation subOp, ShardOperation.OpData opData
    ) {

        ShardOperation shardOpLatest = opStore.getShardOp(opData.getOperationId());
        shardOpLatest.update(opData);
        opStore.updateShardOp(shardOpLatest);

        List<ShardOperation> shardOps = new ArrayList<>();
        // db fetch can be avoided if it is a single sharded subscription i.e. SubscriptionUnitShard strategy.
        if (1 == shardOpLatest.getOpData().getSubscription().getShards().getShardCount()) {
            shardOps.add(shardOpLatest);
        } else {
            shardOps.addAll(opStore.getShardOps(shardOpLatest.getOpData().getParentOpId()));
        }
        subOp.update(shardOps);
        opStore.updateSubOp(subOp);
        return CompletableFuture.completedFuture(null);
    }

    @AllArgsConstructor
    static class OpTask implements Callable<Void> {
        Function<SubscriptionOperation, CompletableFuture<Void>> opHandler;
        private SubscriptionOperation operation;

        public static OpTask of(
                SubscriptionOperation operation, Function<SubscriptionOperation, CompletableFuture<Void>> handler
        ) {
            return new OpTask(handler, operation);
        }

        @Override
        public Void call() {
            //TODO::Fix what happens when opExecutor.apply fails.
            opHandler.apply(operation);
            return null;
        }
    }
}
