package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.entities.cluster.OrderedOperation;
import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.spi.db.MetaStoreException;
import com.flipkart.varadhi.spi.db.OpStore;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class OperationMgr {
    private final OpStore opStore;
    private final ExecutorService executor;
    private final ScheduledExecutorService delayedScheduler;
    private final Map<String, RetryOpTask> retryOpTasks;
    private final Map<String, Deque<OpTask>> opTasks;
    private final RetryPolicy retryPolicy;

    public OperationMgr(int maxConcurrentOps, OpStore opStore, RetryPolicy retryPolicy) {
        this.opStore = opStore;
        this.opTasks = new ConcurrentHashMap<>();
        this.retryOpTasks = new ConcurrentHashMap<>();
        this.retryPolicy = retryPolicy;
        //TODO::ExecutorService should emit the metrics.
        this.executor = Executors.newFixedThreadPool(
                maxConcurrentOps,
                new ThreadFactoryBuilder().setNameFormat("OpMgr-%d").build()
        );
        this.delayedScheduler = Executors.newSingleThreadScheduledExecutor();
    }

    /**
     * Operation Execution
     * - Task may embed a Subscription|ConsumerNode|Shard operation to be executed
     * - Task are keyed by orderingKey -- which is unique for the given entity (derived from its unique id). An entity can
     * have multiple tasks waiting be executed. However only one will be executing at any given instance.
     * - Tasks for different entities can be executed in parallel. Parallelism is controlled via number of threads
     * in the executor service.
     * - Task at the head of the Task queue will be in progress and rest will be waiting.
     * enqueueOpTask -- adds the operation to the queue. If it is only task, it will be immediately scheduled for execution.
     * processOpTaskForOpUpdate -- Removes the tasks from the queue, if finished. If current task is finished, schedule
     * next task for execution if available.
     * TODO:: Implementation can be enhanced for below
     * - Remove redundant operations from the queue.
     */

    private void enqueueOpTask(OpTask pendingTask) {
        opTasks.compute(pendingTask.getOrderingKey(), (orderingKey, scheduledTasks) -> {
            // clear retry tasks which are waiting, as previous failed operation would
            // become invalid with more recent operation being queued.
            pendingTask.clearPendingRetry();

            if (null == scheduledTasks) {
                Deque<OpTask> taskQueue = new ArrayDeque<>();
                taskQueue.addLast(pendingTask);
                pendingTask.execute();
                return taskQueue;
            } else {
                // it means already some operations are scheduled, add this to queue.
                int waitingCount = scheduledTasks.size();
                log.info(
                        "{} waiting operations for key {}, Task({}) queued.", waitingCount,
                        pendingTask.getOrderingKey(), pendingTask
                );

                // When retry and new operation are competing together -- retry scheduled execution and new operation
                // both execute "enqueue" at same time.
                // Ignore retry, as it is invalid now. Retry shouldn't wait when they are ready for execution.
                if (pendingTask.operation.getRetryAttempt() > 0) {
                    pendingTask.markPreempted("Retry aborted, as more recent operation makes it invalid.");
                    log.info(
                            "Ignoring Retry {} enqueue when a another operation {} is in execution, as retry is invalid now.",
                            pendingTask, scheduledTasks.peekFirst()
                    );
                    return scheduledTasks;
                }

                // duplicate shouldn't happen unless it is called multiple times e.g. as part of retry.
                boolean alreadyScheduled = false;
                for (OpTask task : scheduledTasks) {
                    if (task.operation.getId().equals(pendingTask.getId())) {
                        log.warn("Task({}) is already scheduled. Ignoring duplicate.", pendingTask);
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


    // This will execute the update on a Task in a sequential order.
    // Sequential execution is needed, to ensure parallel updates to tasks do not override each other.
    // Synchronous execution in updateHandler is assumed.
    private void processOpTaskForOpUpdate(
            OrderedOperation operation, Function<OrderedOperation, OrderedOperation> updateHandler
    ) {
        opTasks.compute(operation.getOrderingKey(), (orderingKey, scheduledTasks) -> {
            if (null != scheduledTasks && !scheduledTasks.isEmpty()) {
                // validate updates are for in-progress task.
                OpTask opTask = scheduledTasks.peekFirst();
                if (!operation.getId().equals(opTask.getId())) {
                    // This shouldn't happen as only operation at the head of group is scheduled for execution.
                    log.error(
                            "Obtained update for waiting Operation, Updated({}), InProgress({}).", operation,
                            opTask
                    );
                    return scheduledTasks;
                }

                // process the update using provided handler.
                // Update processing can take time, this will affect a subscription.
                processOpUpdate(opTask, updateHandler);

                if (!opTask.operation.isDone()) {
                    log.info("Pending Task({}) still in progress", opTask);
                    return scheduledTasks;
                }

                // Remove completed operation from the pending list and schedule next operation if available.
                return handleCompletedTask(opTask, scheduledTasks);
            } else {
                log.error("No pending Task {} found for update.", operation);
                return null;
            }
        });
    }

    private Deque<OpTask> handleCompletedTask(OpTask completed, Deque<OpTask> taskQueue) {
        taskQueue.remove(completed);
        log.info("Completed Task({}) removed from the queue.", completed);

        // only latest operation should be retried.
        // if this subscription already has operations pending for execution, it will make retry of this operation
        // invalid. Do not schedule retry for such cases.
        OpTask waiting = taskQueue.peekFirst();
        if (null == waiting) {
            log.info("No more pending operation for key {}.", completed.getOrderingKey());
            // retry if this operation has failed.
            completed.enqueueRetryIfFailed();
            return null;
        } else {
            if (completed.operation.hasFailed()) {
                log.info("{} has pending operation {}. Retry will be invalid, skipping retry.", completed, waiting);
            }
            log.info("Next pending Task({}) scheduled for execution.", waiting);
            waiting.execute();
            return taskQueue;
        }
    }

    // for testing purpose.
    List<OrderedOperation> getPendingOperations(String orderingKey) {
        List<OrderedOperation> pendOps = new ArrayList<>();
        if (opTasks.containsKey(orderingKey)) {
            opTasks.get(orderingKey).forEach(opTask -> pendOps.add(opTask.operation));
        }
        return pendOps;
    }

    // for testing purpose.
    RetryOpTask getRetryOperations(String orderingKey) {
        return retryOpTasks.get(orderingKey);
    }


    private void processOpUpdate(OpTask opTask, Function<OrderedOperation, OrderedOperation> updateHandler) {
        if (null != updateHandler) {
            try {
                opTask.operation = updateHandler.apply(opTask.operation);
            } catch (Exception e) {
                log.error(
                        "Operation ({}) had an unexpected failure ({}) during update.", opTask.operation,
                        e.getMessage()
                );
                opTask.saveFailure(e.getMessage());
            }
        }
    }

    void enqueue(SubscriptionOperation subOp, OpExecutor<OrderedOperation> opExecutor) {
        OpTask opTask = new OpTask(opExecutor, op -> opStore.updateSubOp((SubscriptionOperation) op), subOp);
        enqueueOpTask(opTask);
    }

    void createAndEnqueue(SubscriptionOperation subOp, OpExecutor<OrderedOperation> opExecutor) {
        opStore.createSubOp(subOp);
        enqueue(subOp, opExecutor);
    }

    public void submitShardOp(ShardOperation shardOp, boolean isRetry) {
        if (isRetry) {
            shardOp.reset();
            opStore.updateShardOp(shardOp);
        } else if (!opStore.shardOpExists(shardOp.getId())) {
            opStore.createShardOp(shardOp);
        }
    }

    public void updateSubOp(SubscriptionOperation operation) {
        processOpTaskForOpUpdate(operation, op -> {
            // updating DB status in handler, to avoid version conflict.
            return saveSubOpUpdateToStore((SubscriptionOperation) op);
        });
    }

    public void updateShardOp(ShardOperation operation) {
        updateShardOp(
                operation.getOpData().getParentOpId(), operation.getId(), operation.getState(),
                operation.getErrorMsg()
        );
    }

    public void updateShardOp(String subOpId, String shardOpId, ShardOperation.State state, String errorMsg) {
        SubscriptionOperation subscriptionOp = opStore.getSubOp(subOpId);
        // updating DB status in handler for both Shard and Subscription op, to avoid version conflict.
        processOpTaskForOpUpdate(
                subscriptionOp,
                subOp -> updateShardAndSubOp((SubscriptionOperation) subOp, shardOpId, state, errorMsg)
        );
    }

    List<SubscriptionOperation> getPendingSubOps() {
        return opStore.getPendingSubOps();
    }


    public Map<Integer, ShardOperation> getShardOps(String subOpId) {
        return opStore.getShardOps(subOpId).stream().collect(Collectors.toMap(o -> o.getOpData().getShardId(), o -> o));
    }

    private SubscriptionOperation updateShardAndSubOp(
            SubscriptionOperation subOp, String shardOpId, ShardOperation.State state, String errorMsg
    ) {
        ShardOperation shardOpLatest = opStore.getShardOp(shardOpId);
        shardOpLatest.update(state, errorMsg);
        opStore.updateShardOp(shardOpLatest);
        subOp.update(opStore.getShardOps(subOp.getId()));
        return saveSubOpUpdateToStore(subOp);
    }

    private SubscriptionOperation saveSubOpUpdateToStore(SubscriptionOperation subOp) {
        SubscriptionOperation subOpLatest = opStore.getSubOp(subOp.getData().getOperationId());
        subOpLatest.update(subOp.getState(), subOp.getErrorMsg());
        opStore.updateSubOp(subOpLatest);
        return subOpLatest;
    }

    @RequiredArgsConstructor
    class RetryOpTask {
        final OpTask opTask;
        ScheduledFuture<Void> scheduledFuture;

        void schedule() {
            int backOffSeconds = retryPolicy.getRetryBackoffSeconds(opTask.operation);
            scheduledFuture = delayedScheduler.schedule(() -> {
                // task is getting scheduled for execution, remove it from retry pending.
                retryOpTasks.remove(opTask.getOrderingKey());
                enqueueOpTask(opTask);
                return null;
            }, backOffSeconds, TimeUnit.SECONDS);

            retryOpTasks.compute(opTask.getOrderingKey(), (orderingKey, pendingRetry) -> {
                if (null != pendingRetry) {
                    // This is not expected to happen as pending task is not removed till it's retry is queued.
                    //  Any subsequent operation enqueue should remove pending retries if any.
                    // For unknown case, log the error and cancel previous retry.
                    log.error(
                            "Retry task {} already scheduled for key {}. Cancelling previous task", pendingRetry.opTask,
                            orderingKey
                    );
                    pendingRetry.cancel();
                }
                return this;
            });
            log.info("Scheduled retry task for operation {} in {} seconds.", opTask, backOffSeconds);
        }

        void cancel() {
            // don't interrupt if already  running.
            boolean cancelled = scheduledFuture.cancel(false);
            if (cancelled) {
                log.info("Retry {} cancelled, as more recent operation makes it invalid.", opTask);
                opTask.markPreempted("Retry cancelled, as more recent operation makes it invalid.");
            } else {
                log.info(
                        "Retry task failed to cancel. Completed={} Cancelled={} completed for operation {}",
                        scheduledFuture.isDone(), scheduledFuture.isCancelled(), opTask
                );
            }
        }
    }

    @AllArgsConstructor
    class OpTask {
        final OpExecutor<OrderedOperation> opExecutor;
        final Consumer<OrderedOperation> dbUpdateHandler;
        OrderedOperation operation;

        void execute() {
            CompletableFuture.runAsync(() -> {
                try {
                    opExecutor.execute(operation).exceptionally(t -> {
                        log.error("Operation ({}) had a failure {}.", operation, t.getMessage());
                        fail(t);
                        return null;
                    });
                } catch (Exception e) {
                    log.error("Operation ({}) had an unexpected failure {}.", operation, e.getMessage());
                    fail(e);
                }
            }, executor);
        }

        void enqueueRetryIfFailed() {
            if (!operation.hasFailed()) {
                log.info("Operation {} is success,  retry not required.", operation);
                return;
            }
            if (retryPolicy.canRetry(operation)) {
                try {
                    OrderedOperation retryOp = operation.nextRetry();
                    dbUpdateHandler.accept(retryOp);
                    RetryOpTask task = new RetryOpTask(new OpTask(opExecutor, dbUpdateHandler, retryOp));
                    task.schedule();
                } catch (MetaStoreException e) {
                    log.error("Retry ERROR -- {} not retried due to failure {}", operation, e.getMessage());
                    // failure will be primarily due to DB failure. Task is already removed from the operation queue.
                    // In this case ignore the failure and continue, as a result retry will not be attempted for
                    // this operation, but that's ok.
                }
            } else {
                log.error("Operation {} has failed but further retry is not allowed.", operation);
            }
        }

        private void clearPendingRetry() {
            RetryOpTask retryOps = retryOpTasks.remove(getOrderingKey());
            if (null != retryOps) {
                log.info("Retry task {} cancelled and removed.", retryOps.opTask);
                retryOps.cancel();
            }
        }

        private void markPreempted(String reason) {
            // Used when retry tasks are super seeded by more recent operation.
            // Caller should have already  removed it from respective queue.
            // This just marks the task as failed in DB.
            saveFailure(reason);
        }


        private void fail(Throwable t) {
            processOpTaskForOpUpdate(operation, op -> saveFailure(t.getMessage()));
        }

        OrderedOperation saveFailure(String reason) {
            operation.markFail(reason);
            try {
                // persist the operation failure in database.
                dbUpdateHandler.accept(operation);
            } catch (Exception e) {
                // Any exception thrown here will prevent operation from being removed from the operations queue
                // and thus any subsequent operation for same ordering key will keep waiting.
                log.error("Error {} on persisting operation {} failure {}: ", e.getMessage(), operation, reason);
            }
            return operation;
        }

        @Override
        public String toString() {
            return operation.toString();
        }

        String getId() {
            return operation.getId();
        }

        String getOrderingKey() {
            return operation.getOrderingKey();
        }
    }
}
