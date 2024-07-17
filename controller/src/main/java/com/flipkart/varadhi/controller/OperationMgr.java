package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.controller.config.ControllerConfig;
import com.flipkart.varadhi.entities.cluster.Operation;
import com.flipkart.varadhi.entities.cluster.OrderedOperation;
import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.spi.db.OpStore;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;


//TODO::implement closable.
@Slf4j
public class OperationMgr {
    private final OpStore opStore;
    private final ExecutorService executor;
    private final ScheduledExecutorService delayedScheduler;
    private final Map<String, RetryOpTask> retriableOps;
    private final Map<String, Deque<OpTask>> subOps;
    private final RetryPolicy retryPolicy;

    public OperationMgr(ControllerConfig config, OpStore opStore) {
        this.opStore = opStore;
        this.subOps = new ConcurrentHashMap<>();
        this.retriableOps = new ConcurrentHashMap<>();
        this.retryPolicy = new RetryPolicy();
        //TODO::ExecutorService should emit the metrics.
        this.executor = Executors.newFixedThreadPool(
                config.getMaxConcurrentOps(),
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
     * - Implement retry logic for failed operation to support auto recovery from temporary failure.
     */

    private void enqueueOpTask(OpTask pendingTask) {
        subOps.compute(pendingTask.getOrderingKey(), (orderingKey, scheduledTasks) -> {
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

                // if retry and new operation are competing together, ignore retry as it is invalid now.
                // retry shouldn't wait when they are ready for execution.
                if (pendingTask.operation.getRetryAttempt() > 0) {
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


    // This will execute the update on a Task in a sequential order. Sequential execution is needed, to ensure
    // parallel updates to tasks do not override each other.
    //TODO:: check how processOpTaskForOpUpdate will be executed, which thread. Can blocking execution cause an issue ?
    private void processOpTaskForOpUpdate(OrderedOperation operation, Consumer<OrderedOperation> updateHandler) {
        subOps.compute(operation.getOrderingKey(), (orderingKey, scheduledTasks) -> {
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
            // retry if this operation failed.
            completed.queueRetryIfFailed();
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

    // primarily for testing purpose.
    List<OrderedOperation> getPendingOperations(String orderingKey) {
        List<OrderedOperation> pendOps = new ArrayList<>();
        if (subOps.containsKey(orderingKey)) {
            subOps.get(orderingKey).forEach(opTask -> pendOps.add(opTask.operation));
        }
        return pendOps;
    }

    private void processOpUpdate(OpTask opTask, Consumer<OrderedOperation> updateHandler) {
        if (null != updateHandler) {
            try {
                updateHandler.accept(opTask.operation);
            } catch (Exception e) {
                log.error(
                        "Operation ({}) had an unexpected failure ({}) during update.", opTask.operation,
                        e.getMessage()
                );
                opTask.saveFailure(e);
            }
        }
    }

    void enqueue(SubscriptionOperation subOp, OpExecutor<OrderedOperation> opExecutor) {
        OpTask opTask = new OpTask(subOp, opExecutor, op -> opStore.updateSubOp((SubscriptionOperation) op));
        enqueueOpTask(opTask);
    }

    void createAndEnqueue(SubscriptionOperation subOp, OpExecutor<OrderedOperation> opExecutor) {
        opStore.createSubOp(subOp);
        enqueue(subOp, opExecutor);
    }

    CompletableFuture<Void> createAndExecute(ShardOperation shardOp, OpExecutor<Operation> opExecutor) {
        // shard operation might have been already created during previous execution.
        // create only if needed.
        if (!opStore.shardOpExists(shardOp.getId())) {
            opStore.createShardOp(shardOp);
        }
        return opExecutor.execute(shardOp);
    }

    // used in testing
    void updateSubOp(SubscriptionOperation operation) {
        processOpTaskForOpUpdate(operation, op -> {
            // updating DB status in handler, to avoid version conflict.
            SubscriptionOperation subOp = (SubscriptionOperation) op;
            SubscriptionOperation subOpLatest = opStore.getSubOp(subOp.getData().getOperationId());
            subOpLatest.update(subOp.getState(), subOp.getErrorMsg());
            opStore.updateSubOp(subOpLatest);
        });
    }

    void updateShardOp(String subOpId, String shardOpId, ShardOperation.State state, String errorMsg) {
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


    Map<Integer, ShardOperation> getShardOps(String subOpId) {
        return opStore.getShardOps(subOpId).stream().collect(Collectors.toMap(o -> o.getOpData().getShardId(), o -> o));
    }

    private void updateShardAndSubOp(
            SubscriptionOperation subOp, String shardOpId, ShardOperation.State state, String errorMsg
    ) {
        ShardOperation shardOpLatest = opStore.getShardOp(shardOpId);
        shardOpLatest.update(state, errorMsg);
        opStore.updateShardOp(shardOpLatest);
        subOp.update(opStore.getShardOps(subOp.getId()));
        opStore.updateSubOp(subOp);
    }

    @RequiredArgsConstructor
    class RetryOpTask {
        private final OpTask opTask;
        private ScheduledFuture<Void> scheduledFuture;

        void schedule() {
            int backOffSeconds = retryPolicy.getRetryBackoffSeconds(opTask.operation);
            scheduledFuture = delayedScheduler.schedule(() -> {
                // task is getting scheduled for execution, remove it from retry pending.
                retriableOps.remove(opTask.getOrderingKey());
                enqueueOpTask(opTask);
                return null;
            }, backOffSeconds, TimeUnit.SECONDS);

            retriableOps.compute(opTask.getOrderingKey(), (orderingKey, pendingRetry) -> {
                if (null != pendingRetry) {
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
                log.info("Retry task cancelled for operation {}", opTask);
            } else {
                log.info(
                        "Retry task failed to cancel. Completed={} Cancelled={} completed for operation {}",
                        scheduledFuture.isDone(), scheduledFuture.isCancelled(), opTask
                );
            }
        }
    }


    @RequiredArgsConstructor
    class OpTask {
        private final OrderedOperation operation;
        private final OpExecutor<OrderedOperation> opExecutor;
        private final Consumer<OrderedOperation> dbUpdateHandler;

        void execute() {
            log.info("Scheduled the Operation({}) for execution.", operation);
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

        void queueRetryIfFailed() {
            if (retryPolicy.canRetry(operation)) {
                // not persisting the operation updated for retry.
                // it will get persisted during update on completion. Might result in one extra retry
                // if controller switches, that should be ok.
                OrderedOperation retryOp = operation.nextRetry();
                RetryOpTask task = new RetryOpTask(new OpTask(retryOp, opExecutor, dbUpdateHandler));
                task.schedule();
                log.info("Retry the operation {}.", retryOp);
            }
        }

        private void clearPendingRetry() {
            RetryOpTask retryOps = retriableOps.remove(getOrderingKey());
            if (null != retryOps) {
                log.info("Retry task {} cancelled and removed.", retryOps.opTask);
                retryOps.cancel();
            }
        }


        private void fail(Throwable t) {
            //TODO::Fix in case of DB failure, saveFailure will be called twice
            //1 - as part of fail() processing, 2 - as part of processOpTaskForOpUpdate.opUpdate() failure processing
            // fix this.
            processOpTaskForOpUpdate(operation, op -> saveFailure(t));
        }

        void saveFailure(Throwable t) {
            operation.markFail(t.getMessage());
            try {
                // persist the operation failure in database.
                dbUpdateHandler.accept(operation);
            } catch (Exception e) {
                // Any exception thrown here will prevent operation from being removed from the operations queue
                // and thus any subsequent operation for same ordering key will keep waiting.
                log.error(
                        "Error {} on persisting operation {} failure {}: ", e.getMessage(), operation, t.getMessage());
            }

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
