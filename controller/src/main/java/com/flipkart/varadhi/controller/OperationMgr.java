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
                config.getMaxConcurrentOps(),
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
     * enqueueOpTask -- adds the operation to the queue. If it is only task, it will be immediately scheduled for execution.
     * processOpTaskForOpUpdate -- Removes the tasks from the queue, if finished. If current task is finished, schedule
     * next task for execution if available.
     * TODO:: Implementation can be enhanced for below
     * - Remove redundant operations from the queue.
     * - Implement retry logic for failed operation to support auto recovery from temporary failure.
     */
    private void enqueueOpTask(OpTask pendingTask) {
        subOps.compute(pendingTask.getOrderingKey(), (orderingKey, scheduledTasks) -> {
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

                if (opTask.isRunning()) {
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
        if (taskQueue.isEmpty()) {
            log.info("No more pending operation for key {}.", completed.getOrderingKey());
            return null;
        }
        OpTask waiting = taskQueue.peekFirst();
        log.info("Next pending Task({}) scheduled for execution.", waiting);
        waiting.execute();
        return taskQueue;
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

    public void enqueue(SubscriptionOperation subOp, OpExecutor<OrderedOperation> opExecutor) {
        OpTask opTask = new OpTask(subOp, opExecutor, op -> opStore.updateSubOp((SubscriptionOperation) op));
        enqueueOpTask(opTask);
    }

    public void createAndEnqueue(SubscriptionOperation subOp, OpExecutor<OrderedOperation> opExecutor) {
        opStore.createSubOp(subOp);
        enqueue(subOp, opExecutor);
    }

    public CompletableFuture<Void> createAndExecute(ShardOperation shardOp, OpExecutor<Operation> opExecutor) {
        // shard operation might have been already created during previous execution.
        // create only if needed.
        if (!opStore.shardOpExists(shardOp.getId())) {
            opStore.createShardOp(shardOp);
        }
        return opExecutor.execute(shardOp);
    }

    public void updateSubOp(SubscriptionOperation operation) {
        processOpTaskForOpUpdate(operation, op -> {
            // updating DB status in handler, to avoid version conflict.
            SubscriptionOperation subOp = (SubscriptionOperation) op;
            SubscriptionOperation subOpLatest = opStore.getSubOp(subOp.getData().getOperationId());
            subOpLatest.update(subOp);
            opStore.updateSubOp(subOpLatest);
        });
    }

    public void updateShardOp(ShardOperation.OpData opData) {
        SubscriptionOperation subscriptionOp = opStore.getSubOp(opData.getParentOpId());
        // updating DB status in handler for both Shard and Subscription op, to avoid version conflict.
        processOpTaskForOpUpdate(subscriptionOp, subOp -> updateShardAndSubOp((SubscriptionOperation) subOp, opData));
    }

    public List<SubscriptionOperation> getPendingSubOps() {
        return opStore.getPendingSubOps();
    }


    public Map<Integer, ShardOperation> getShardOps(String subOpId) {
        return opStore.getShardOps(subOpId).stream().collect(Collectors.toMap(o -> o.getOpData().getShardId(), o -> o));
    }

    private void updateShardAndSubOp(
            SubscriptionOperation subOp, ShardOperation.OpData opData
    ) {
        ShardOperation shardOpLatest = opStore.getShardOp(opData.getOperationId());
        shardOpLatest.update(opData);
        opStore.updateShardOp(shardOpLatest);
        subOp.update(opStore.getShardOps(shardOpLatest.getOpData().getParentOpId()));
        opStore.updateSubOp(subOp);
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

        private void fail(Throwable t) {
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

        public String getId() {
            return operation.getId();
        }

        public String getOrderingKey() {
            return operation.getOrderingKey();
        }

        public boolean isRunning() {
            return !operation.isDone();
        }
    }
}
