package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.controller.config.ControllerConfig;
import com.flipkart.varadhi.entities.SubscriptionUnitShard;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.cluster.Assignment;
import com.flipkart.varadhi.entities.cluster.GroupOperation;
import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.spi.db.OpStore;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Function;


@Slf4j
public class OperationMgr {
    private final OpStore opStore;
    private final ExecutorService executor;
    private final Map<String, Deque<OpTask>> subOps;

    public OperationMgr(ControllerConfig config, OpStore opStore) {
        this.opStore = opStore;
        this.subOps = new ConcurrentHashMap<>();
        //TODO::ExecutorService should emit the metrics.
        this.executor = Executors.newFixedThreadPool(config.getOperationExecutionThreads(),
                new ThreadFactoryBuilder().setNameFormat("OpMgr-%d").build()
        );
    }

    /**
     * Operation Execution
     * - Task may embed a Subscription|ConsumerNode|Shard operation to be executed
     * - Task are keyed by groupId -- which is unique for the given entity (derived from its unique id). An entity can
     * have multiple tasks waiting be executed. However only one will be executing at any given instance.
     * - Tasks for different entities can be executed in parallel. Parallelism is controlled via number of threads
     * in the executor service.
     * - Task at the head of the Task queue will be in progress and rest will be waiting.
     * queueTask -- adds the operation to the queue. If it is only task, it will be immediately scheduled for execution.
     * handleOpUpdate -- Removes the tasks from the queue, if finished. If current task is finished, schedule next task
     * for execution if available.
     * TODO:: Implementation can be enhanced for below
     * - Remove redundant operations from the queue.
     * - Implement retry logic for failed operation to support auto recovery from temporary failure.
     */
    private void enqueueOperation(
            GroupOperation operation, Function<GroupOperation, CompletableFuture<Void>> opExecutor
    ) {
        OpTask pendingTask = OpTask.of(operation, opExecutor);
        subOps.compute(operation.getGroupId(), (opGroupId, scheduledTasks) -> {
            if (null == scheduledTasks) {
                Deque<OpTask> taskQueue = new ArrayDeque<>();
                taskQueue.addLast(pendingTask);
                executeOperation(pendingTask);
                return taskQueue;
            } else {
                // it means already some operations are scheduled, add this to queue.
                int waitingTasks = scheduledTasks.size();
                log.info(
                        "OpGroup {} has {} waiting operations, Operation({}) queued.", operation.getGroupId(),
                        waitingTasks,
                        operation
                );
                // duplicate shouldn't happen unless it is called multiple times e.g. as part of retry.
                boolean alreadyScheduled = false;
                for (OpTask task : scheduledTasks) {
                    if (task.operation.getId().equals(pendingTask.operation.getId())) {
                        log.warn("Operation({}) is already scheduled. Ignoring duplicate.", operation);
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

    private void executeOperation(OpTask task) {
        log.info("Scheduled the Operation({}) for execution.", task.operation);
        CompletableFuture.supplyAsync(() -> task, executor).exceptionally(t -> {
            log.error("Unexpected failure({}) during execution of operation ({}).", t.getMessage(), task.operation);
            task.operation.markFail(t.getMessage());
            handleOpUpdate(task.operation, op -> failWithUnhandledException(op, t));
            return task;
        });
    }


    // This will execute the update on a Task in a sequential order. Sequential execution is needed, to ensure
    // parallel updates to tasks do not override each other.
    //TODO:: check how handleOpUpdate will be executed, which thread. Can blocking execution cause an issue ?
    private void handleOpUpdate(GroupOperation operation, Consumer<GroupOperation> updateHandler) {
        subOps.compute(operation.getGroupId(), (opGroupId, scheduledTasks) -> {
            if (null != scheduledTasks && !scheduledTasks.isEmpty()) {
                // process the update using provided handler.
                // Update processing can take time, this will affect a subscription.
                updateOperation(operation, updateHandler);

                GroupOperation inProgress = scheduledTasks.peekFirst().operation;
                if (!operation.getId().equals(inProgress.getId())) {
                    // This shouldn't happen as only operation at the head of group is scheduled for execution.
                    log.error(
                            "Obtained update for waiting Operation, Updated({}), InProgress({}).", operation,
                            inProgress
                    );
                    return scheduledTasks;
                }

                // Remove completed operation from the pending list and schedule next operation if available.
                if (operation.isDone()) {
                    scheduledTasks.removeFirst();
                    log.info("Completed Operation({}) removed from the OpGroup.", operation);
                    if (scheduledTasks.isEmpty()) {
                        log.info("No more pending operation for OpGroup{}.", opGroupId);
                        return null;
                    } else {
                        OpTask waiting = scheduledTasks.peekFirst();
                        log.info("Next pending Operation({}) scheduled for execution.", waiting.operation);
                        executeOperation(waiting);
                    }
                } else {
                    log.info("Pending Operation({}) still in progress", operation);
                }
                return scheduledTasks;
            } else {
                log.error("Operation {} not found for update.", operation);
                return null;
            }
        });
    }

    private void updateOperation(GroupOperation operation, Consumer<GroupOperation> updateHandler) {
        if (null != updateHandler) {
            try {
                updateHandler.accept(operation);
            } catch (Exception e) {
                log.error("Unhandled failure in Operation({}) update:{}, marking it failed.", operation, e);
                failWithUnhandledException(operation, e);
            }
        }
    }

    public SubscriptionOperation requestSubStart(
            String subscriptionId, String requestedBy,
            Function<GroupOperation, CompletableFuture<Void>> provider
    ) {
        SubscriptionOperation operation = SubscriptionOperation.startOp(subscriptionId, requestedBy);
        opStore.createSubOp(operation);
        enqueueOperation(operation, provider);
        return operation;
    }

    public SubscriptionOperation requestSubStop(
            String subscriptionId, String requestedBy,
            Function<GroupOperation, CompletableFuture<Void>> provider
    ) {
        SubscriptionOperation operation = SubscriptionOperation.stopOp(subscriptionId, requestedBy);
        opStore.createSubOp(operation);
        enqueueOperation(operation, provider);
        return operation;
    }

    public SubscriptionOperation requestShardReassign(
            Assignment assignment, String requestedBy, Function<GroupOperation, CompletableFuture<Void>> provider
    ) {
        SubscriptionOperation operation = SubscriptionOperation.reAssignShardOp(assignment, requestedBy);
        opStore.createSubOp(operation);
        enqueueOperation(operation, provider);
        return operation;
    }

    public void updateSubOp(SubscriptionOperation subscriptionOp) {
        handleOpUpdate(subscriptionOp, subOp -> {
            // updating DB status in handler, to avoid version conflict.
            SubscriptionOperation subOpLatest = opStore.getSubOp(subscriptionOp.getData().getOperationId());
            subOpLatest.update(subscriptionOp);
            opStore.updateSubOp(subOpLatest);
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
        handleOpUpdate(subscriptionOp, subOp -> updateShardAndSubOp((SubscriptionOperation) subOp, opData));
    }

    private void updateShardAndSubOp(
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
    }

    private void failWithUnhandledException(GroupOperation operation, Throwable t) {
        //TODO:: better alternative is needed.
        operation.markFail(t.getMessage());
        try {
            if (operation instanceof SubscriptionOperation) {
                opStore.updateSubOp((SubscriptionOperation) operation);
            } else if (operation instanceof ShardOperation) {
                opStore.updateShardOp((ShardOperation) operation);
            }
        } catch (Exception e) {
            // Any exception thrown here will prevent operation from being removed from the operations queue
            // and thus any subsequent operation will keep waiting.
            log.error("Error while persisting operation: {}", operation, e);
        }
    }

    @AllArgsConstructor
    static class OpTask implements Callable<CompletableFuture<Void>> {
        private GroupOperation operation;
        private Function<GroupOperation, CompletableFuture<Void>> opHandler;

        public static OpTask of(GroupOperation operation, Function<GroupOperation, CompletableFuture<Void>> handler) {
            return new OpTask(operation, handler);
        }

        @Override
        public CompletableFuture<Void> call() {
            try {
                return opHandler.apply(operation);
            } catch (Exception e) {
                return CompletableFuture.failedFuture(e);
            }
        }
    }
}
