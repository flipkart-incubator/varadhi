package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.controller.config.ControllerConfig;
import com.flipkart.varadhi.entities.cluster.Operation;
import com.flipkart.varadhi.entities.cluster.OrderedOperation;
import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.spi.db.OpStore;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;


@Slf4j
public class OperationMgr {
    private final OpStore opStore;
    private final ExecutorService executor;
    private final Map<String, Deque<OpTask>> subOps;

    public OperationMgr(ControllerConfig config, OpStore opStore) {
        this.opStore = opStore;
        this.subOps = new ConcurrentHashMap<>();
        //TODO::ExecutorService should emit the metrics.
        this.executor = Executors.newFixedThreadPool(
                config.getOperationExecutionThreads(),
                new ThreadFactoryBuilder().setNameFormat("OpMgr-%d").build()
        );
    }

    /**
     * Operation Execution
     * - Task may embed a Subscription|ConsumerNode|Shard operation to be executed
     * - Task are keyed by orderingKey -- which is unique for the given entity (derived from its unique id). An entity can
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
    private void enqueueOperation(OrderedOperation operation, OpExecutor<OrderedOperation> opExecutor) {
        OpTask pendingTask = OpTask.of(operation, opExecutor);
        subOps.compute(operation.getOrderingKey(), (orderingKey, scheduledTasks) -> {
            if (null == scheduledTasks) {
                Deque<OpTask> taskQueue = new ArrayDeque<>();
                taskQueue.addLast(pendingTask);
                executeOperation(pendingTask);
                return taskQueue;
            } else {
                // it means already some operations are scheduled, add this to queue.
                int waitingTasks = scheduledTasks.size();
                log.info("{} waiting operations for key {}, Operation({}) queued.", waitingTasks, operation.getOrderingKey(), operation);
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
        CompletableFuture.runAsync(() -> {
            try {
                task.call().exceptionally(t -> {
                    log.error("Operation ({}) had a failure ({}).", task.operation, t.getMessage());
                    failWithException(task.operation, t);
                    return null;
                });
            } catch (Exception e) {
                handleOpUpdate(task.operation, op -> {
                    log.error("Operation ({}) had an unexpected failure ({}).", op, e.getMessage());
                    failWithException(op, e);
                });
            }
        }, executor);
    }


    // This will execute the update on a Task in a sequential order. Sequential execution is needed, to ensure
    // parallel updates to tasks do not override each other.
    //TODO:: check how handleOpUpdate will be executed, which thread. Can blocking execution cause an issue ?
    private void handleOpUpdate(OrderedOperation operation, Consumer<OrderedOperation> updateHandler) {
        subOps.compute(operation.getOrderingKey(), (orderingKey, scheduledTasks) -> {
            if (null != scheduledTasks && !scheduledTasks.isEmpty()) {
                // process the update using provided handler.
                // Update processing can take time, this will affect a subscription.
                updateOperation(operation, updateHandler);

                OrderedOperation inProgress = scheduledTasks.peekFirst().operation;
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
                    log.info("Completed Operation({}) removed from the queue.", operation);
                    if (scheduledTasks.isEmpty()) {
                        log.info("No more pending operation for key{}.", orderingKey);
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

    private void updateOperation(OrderedOperation operation, Consumer<OrderedOperation> updateHandler) {
        if (null != updateHandler) {
            try {
                updateHandler.accept(operation);
            } catch (Exception e) {
                log.error("Operation ({}) had an unexpected failure ({}) during update.", operation, e.getMessage());
                failWithException(operation, e);
            }
        }
    }

    public void createAndEnqueue(SubscriptionOperation subOp, OpExecutor<OrderedOperation> opExecutor) {
        opStore.createSubOp(subOp);
        enqueueOperation(subOp, opExecutor);
    }
    public CompletableFuture<Void> createAndExecute(ShardOperation shardOp, OpExecutor<Operation> opExecutor) {
        opStore.createShardOp(shardOp);
        return opExecutor.execute(shardOp);
    }
    public void updateSubOp(SubscriptionOperation subscriptionOp) {
        handleOpUpdate(subscriptionOp, subOp -> {
            // updating DB status in handler, to avoid version conflict.
            SubscriptionOperation subOpLatest = opStore.getSubOp(subscriptionOp.getData().getOperationId());
            subOpLatest.update(subscriptionOp);
            opStore.updateSubOp(subOpLatest);
        });
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

    private void failWithException(OrderedOperation operation, Throwable t) {
        operation.markFail(t.getMessage());
        try {
            //TODO:: better alternative is needed.
            if (operation instanceof SubscriptionOperation) {
                opStore.updateSubOp((SubscriptionOperation) operation);
            } else if (operation instanceof ShardOperation) {
                opStore.updateShardOp((ShardOperation) operation);
            }
        } catch (Exception e) {
            // Any exception thrown here will prevent operation from being removed from the operations queue
            // and thus any subsequent operation for same ordering key will keep waiting.
            log.error("Error while persisting operation: {}", operation, e);
        }
    }

    @AllArgsConstructor
    static class OpTask implements Callable<CompletableFuture<Void>> {
        private OrderedOperation operation;
        private OpExecutor<OrderedOperation> opExecutor;

        public static OpTask of(OrderedOperation operation, OpExecutor<OrderedOperation> opExecutor) {
            return new OpTask(operation, opExecutor);
        }

        @Override
        public CompletableFuture<Void> call() {
            return opExecutor.execute(operation);
        }
    }
}
