package com.flipkart.varadhi.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.flipkart.varadhi.entities.cluster.*;
import com.flipkart.varadhi.core.cluster.*;
import com.flipkart.varadhi.entities.SubscriptionShards;
import com.flipkart.varadhi.entities.SubscriptionUnitShard;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.spi.db.MetaStore;
import lombok.extern.slf4j.Slf4j;

import static com.flipkart.varadhi.Constants.SYSTEM_IDENTITY;
import static com.flipkart.varadhi.entities.cluster.Operation.State.ERRORED;

@Slf4j
public class ControllerApiMgr implements ControllerApi {
    private final AssignmentManager assignmentManager;
    private final ConsumerClientFactory consumerClientFactory;
    private final MetaStore metaStore;
    private final OperationMgr operationMgr;

    public ControllerApiMgr(
            OperationMgr operationMgr, AssignmentManager assignmentManager, MetaStore metaStore,
            ConsumerClientFactory consumerClientFactory
    ) {
        this.consumerClientFactory = consumerClientFactory;
        this.assignmentManager = assignmentManager;
        this.metaStore = metaStore;
        this.operationMgr = operationMgr;
    }

    public CompletableFuture<String> addConsumerNode(ConsumerNode consumerNode) {
        return getConsumerInfo(consumerNode.getConsumerId()).thenApply(ci -> {
            consumerNode.initFromConsumerInfo(ci);
            assignmentManager.addConsumerNode(consumerNode);
            return consumerNode.getConsumerId();
        });
    }

    @Override
    public CompletableFuture<SubscriptionStatus> getSubscriptionStatus(String subscriptionId, String requestedBy) {
        VaradhiSubscription subscription = metaStore.getSubscription(subscriptionId);
        return getSubscriptionStatus(subscription).exceptionally(t -> {
            throw new IllegalStateException(
                    String.format(
                            "Failure in getting subscription status, try again after sometime. %s",
                            t.getMessage()
                    ));
        });
    }

    CompletableFuture<SubscriptionStatus> getSubscriptionStatus(VaradhiSubscription subscription) {
        String subId = subscription.getName();
        List<Assignment> assignments = assignmentManager.getSubAssignments(subId);

        List<CompletableFuture<ShardStatus>> shardFutures = assignments.stream().map(a -> {
            ConsumerApi consumer = getAssignedConsumer(a);
            return consumer.getShardStatus(subId, a.getShardId());
        }).toList();

        return CompletableFuture.allOf(shardFutures.toArray(CompletableFuture[]::new)).thenApply(v -> {
            List<ShardStatus> shardStatuses = new ArrayList<>();
            shardFutures.forEach(sf -> shardStatuses.add(sf.join()));
            return getSubscriptionStatusFromShardStatus(subscription, assignments, shardStatuses);
        });
    }

    private SubscriptionStatus getSubscriptionStatusFromShardStatus(
            VaradhiSubscription subscription, List<Assignment> assignments, List<ShardStatus> shardStatuses
    ) {
        SubscriptionState state = SubscriptionState.getFromShardStates(assignments, shardStatuses);
        return new SubscriptionStatus(subscription.getName(), state);
    }

    @Override
    public CompletableFuture<SubscriptionOperation> startSubscription(
            String subscriptionId, String requestedBy
    ) {
        VaradhiSubscription subscription = metaStore.getSubscription(subscriptionId);
        return getSubscriptionStatus(subscription).exceptionally(t -> {
            // If not temporary, then alternate needs to be provided to allow recovery from this.
            throw new IllegalStateException(
                    String.format(
                            "Failure in getting subscription status, try again after sometime. %s",
                            t.getMessage()
                    ));
        }).thenApply(ss -> {
            if (ss.getState() == SubscriptionState.RUNNING || ss.getState() == SubscriptionState.STARTING) {
                throw new InvalidOperationForResourceException("Subscription is either already running or starting.");
            }
            log.info("Starting the Subscription: {}", subscriptionId);
            SubscriptionOperation operation = SubscriptionOperation.startOp(subscriptionId, requestedBy);
            operationMgr.createAndEnqueue(operation, new StartOpExecutor(subscription));
            return operation;
        });
    }

    private CompletableFuture<Void> startShards(SubscriptionOperation subOp, VaradhiSubscription subscription) {
        SubscriptionShards shards = subscription.getShards();
        return getOrCreateShardAssignment(subscription).thenCompose(assignments -> {
            List<CompletableFuture<Boolean>> shardFutures = scheduleStartOnShards(subscription, subOp, assignments);
            log.info("Executed Start on {} shards for SubOp({}).", shards.getShardCount(), subOp.getData());
            return CompletableFuture.allOf(shardFutures.toArray(new CompletableFuture[0])).thenApply(ignore -> {
                if (allShardsSkipped(shardFutures)) {
                    log.info("Start {} completed without any shard operations being scheduled.", subOp.getData());
                    completeSubOperation(subOp);
                }
                return null;
            });
        });
    }

    private CompletableFuture<List<Assignment>> getOrCreateShardAssignment(VaradhiSubscription subscription) {
        List<SubscriptionUnitShard> unAssigned = getSubscriptionShards(subscription.getShards());
        return assignmentManager.assignShards(unAssigned, subscription, List.of());
    }

    private List<CompletableFuture<Boolean>> scheduleStartOnShards(
            VaradhiSubscription subscription, SubscriptionOperation subOp, List<Assignment> assignments
    ) {
        SubscriptionShards shards = subscription.getShards();
        String subOpId = subOp.getData().getOperationId();
        Map<Integer, ShardOperation> shardOps = operationMgr.getShardOps(subOpId);
        return assignments.stream().map(assignment -> {
            ConsumerApi consumer = getAssignedConsumer(assignment);
            SubscriptionUnitShard shard = shards.getShard(assignment.getShardId());
            ShardOperation shardOp = shardOps.computeIfAbsent(
                    assignment.getShardId(),
                    shardId -> ShardOperation.startOp(subOpId, shard, subscription)
            );
            return startShard(shardOp, subOp.isInRetry(), consumer);
        }).toList();
    }

    private CompletableFuture<Boolean> startShard(ShardOperation startOp, boolean isRetry, ConsumerApi consumer) {
        String subId = startOp.getOpData().getSubscriptionId();
        int shardId = startOp.getOpData().getShardId();
        return consumer.getShardStatus(subId, shardId).thenCompose(shardStatus -> {
            // Start can be executed in stopping subscription as well.
            // in general this shouldn't happen as multiple in-progress operations are not allowed.
            if (shardStatus.isStarted()) {
                log.info("Subscription:{} Shard:{} is already started. Skipping.", subId, shardId);
                return CompletableFuture.completedFuture(false);
            }
            operationMgr.createOrResetShardOp(startOp, isRetry);
            CompletableFuture<Boolean> startFuture =
                    consumer.start((ShardOperation.StartData) startOp.getOpData()).thenApply(v -> true);
            log.info("Scheduled shard start({}).", startOp);
            return startFuture;
        }).exceptionally(t -> {
            failShardOp(startOp, t);
            return true;
        });
    }


    @Override
    public CompletableFuture<SubscriptionOperation> stopSubscription(
            String subscriptionId, String requestedBy
    ) {
        VaradhiSubscription subscription = metaStore.getSubscription(subscriptionId);
        return getSubscriptionStatus(subscription).exceptionally(t -> {
            throw new IllegalStateException(
                    String.format(
                            "Failure in getting subscription status, try again after sometime. %s",
                            t.getMessage()
                    ));
        }).thenApply(ss -> {
            if (ss.getState() == SubscriptionState.STOPPED || ss.getState() == SubscriptionState.STOPPING) {
                throw new InvalidOperationForResourceException("Subscription is either already stopped or stopping.");
            }
            log.info("Stopping the Subscription: {}", subscriptionId);
            SubscriptionOperation operation = SubscriptionOperation.stopOp(subscriptionId, requestedBy);
            operationMgr.createAndEnqueue(operation, new StopOpExecutor(subscription));
            return operation;
        });
    }

    private CompletableFuture<Void> stopShards(SubscriptionOperation subOp, VaradhiSubscription subscription) {
        String subId = subscription.getName();
        SubscriptionShards shards = subscription.getShards();
        List<Assignment> assignments = assignmentManager.getSubAssignments(subId);
        log.info(
                "Found {} assigned Shards for the Subscription:{} with total {} Shards.", assignments.size(),
                subId, shards.getShardCount()
        );

        List<Assignment> stopsFailed = new ArrayList<>();
        List<CompletableFuture<Boolean>> shardFutures =
                scheduleStopOnShards(subscription, subOp, assignments, stopsFailed);
        log.info("Executed Stop on {} shards for SubOp({}).", shards.getShardCount(), subOp.getData());

        // in case assignments is empty i.e. no assignment exists for this subscription.
        // nothing special is needed. Default flow will take care of marking operation complete.
        return CompletableFuture.allOf(shardFutures.toArray(new CompletableFuture[0])).thenCompose(v -> {
            // unAssignShards shouldn't be called for shards which failed to stop.
            stopsFailed.forEach(assignments::remove);
            return assignmentManager.unAssignShards(assignments, subscription, true);
        }).thenApply(ignore -> {
            if (allShardsSkipped(shardFutures)) {
                log.info("Stop {} completed without any shard operations being scheduled.", subOp.getData());
                completeSubOperation(subOp);
            }
            return null;
        });
    }

    private List<CompletableFuture<Boolean>> scheduleStopOnShards(
            VaradhiSubscription subscription, SubscriptionOperation subOp, List<Assignment> assignments,
            List<Assignment> stopsFailed
    ) {
        SubscriptionShards shards = subscription.getShards();
        String subOpId = subOp.getData().getOperationId();
        Map<Integer, ShardOperation> shardOps = operationMgr.getShardOps(subOpId);
        return assignments.stream().map(assignment -> {
            ConsumerApi consumer = getAssignedConsumer(assignment);
            SubscriptionUnitShard shard = shards.getShard(assignment.getShardId());
            ShardOperation shardOp = shardOps.computeIfAbsent(
                    assignment.getShardId(),
                    shardId -> ShardOperation.stopOp(subOpId, shard, subscription)
            );
            return stopShard(shardOp, subOp.isInRetry(), consumer).whenComplete((scheduled, throwable) -> {
                if (throwable != null || shardOp.hasFailed()) {
                    stopsFailed.add(assignment);
                }
            });
        }).toList();
    }

    private CompletableFuture<Boolean> stopShard(ShardOperation stopOp, boolean isRetry, ConsumerApi consumer) {
        String subId = stopOp.getOpData().getSubscriptionId();
        int shardId = stopOp.getOpData().getShardId();
        return consumer.getShardStatus(subId, shardId).thenCompose(shardStatus -> {
            // Stop can be executed in starting subscription as well.
            // in general this shouldn't happen as multiple in-progress operations are not allowed.
            if (shardStatus.isStopped()) {
                log.info("Subscription:{} Shard:{} is already Stopped. Skipping.", subId, shardId);
                return CompletableFuture.completedFuture(false);
            }
            operationMgr.createOrResetShardOp(stopOp, isRetry);
            CompletableFuture<Void> stopFuture = consumer.stop((ShardOperation.StopData) stopOp.getOpData());
            log.info("Scheduled shard stop({}).", stopOp);
            return stopFuture.thenApply(v -> true);
        }).exceptionally(t -> {
            failShardOp(stopOp, t);
            return true;
        });
    }

    private boolean allShardsSkipped(List<CompletableFuture<Boolean>> shardFutures) {
        long notScheduled = shardFutures.stream().filter(f -> {
            try {
                return f.isDone() && !f.get();
            } catch (Exception e) {
                // this is not expected.
                log.error(
                        "Unexpected error while retrieving the shard operation scheduling status {}.", e.getMessage());
                throw new RuntimeException(e);
            }
        }).count();
        log.info("Shards total:{} operation not scheduled: {}.", shardFutures.size(), notScheduled);
        return notScheduled == shardFutures.size();
    }

    private void failShardOp(ShardOperation shardOp, Throwable t) {
        log.error("shard operation ({}) failed: {}.", shardOp, t.getMessage());
        operationMgr.updateShardOp(shardOp.getOpData().getParentOpId(), shardOp.getId(), ERRORED, t.getMessage());
    }

    private void completeSubOperation(SubscriptionOperation subOp) {
        subOp.markCompleted();
        operationMgr.updateSubOp(subOp);
    }

    private ConsumerApi getAssignedConsumer(Assignment assignment) {
        return consumerClientFactory.getInstance(assignment.getConsumerId());
    }

    private List<SubscriptionUnitShard> getSubscriptionShards(SubscriptionShards shards) {
        List<SubscriptionUnitShard> unitShards = new ArrayList<>();
        for (int shardId = 0; shardId < shards.getShardCount(); shardId++) {
            SubscriptionUnitShard shard = shards.getShard(shardId);
            unitShards.add(shard);
        }
        return unitShards;
    }


    @Override
    public CompletableFuture<Void> update(
            String subOpId, String shardOpId, ShardOperation.State state, String errorMsg
    ) {
        log.info(
                "Received update on shard operation: SubOpId={} ShardOpId={}, State={}, Error={}", subOpId, shardOpId,
                state, errorMsg
        );
        try {
            // Update is getting executed inline on dispatcher thread.
            operationMgr.updateShardOp(subOpId, shardOpId, state, errorMsg);
            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    public CompletableFuture<Void> consumerNodeLeft(String consumerNodeId) {
        log.info("ConsumerNode {} left the cluster.", consumerNodeId);
        return assignmentManager.consumerNodeLeft(consumerNodeId).thenAccept((v) -> {
            List<Assignment> assignments = assignmentManager.getConsumerNodeAssignments(consumerNodeId);
            assignments.forEach(assignment -> {
                log.info("Assignment {} needs to be re-assigned", assignment);
                SubscriptionOperation operation = SubscriptionOperation.reAssignShardOp(assignment, SYSTEM_IDENTITY);
                operationMgr.createAndEnqueue(operation, new ReAssignOpExecutor());
            });
        });
    }

    /**
     * Re-Assigns shard to a Consumer Node different from current assignment.
     * Re-Assign = UnAssign -> Assign.
     * If there is a failure during Assign, Subscription shard remains un-assigned.
     * -- Retry (Auto or manual) of failed Re-Assign operation or start of the subscription should fix this.
     */
    private CompletableFuture<Void> reAssignShard(SubscriptionOperation subOp) {
        SubscriptionOperation.ReassignShardData data = (SubscriptionOperation.ReassignShardData) subOp.getData();
        Assignment currentAssignment = data.getAssignment();
        VaradhiSubscription subscription = metaStore.getSubscription(currentAssignment.getSubscriptionId());
        Map<Integer, ShardOperation> shardOps = operationMgr.getShardOps(subOp.getId());
        return assignmentManager.reAssignShard(currentAssignment, subscription, false).thenCompose(a -> {
            ConsumerApi consumer = getAssignedConsumer(a);
            SubscriptionUnitShard shard = subscription.getShards().getShard(currentAssignment.getShardId());
            ShardOperation shardOp = shardOps.computeIfAbsent(
                    a.getShardId(),
                    shardId -> ShardOperation.startOp(subOp.getId(), shard, subscription)
            );
            return startShard(shardOp, subOp.isInRetry(), consumer).thenApply(startScheduled -> {
                if (!startScheduled) {
                    log.info("ReAssign {} completed without any shard operations being scheduled.", subOp.getData());
                    completeSubOperation(subOp);
                } else {
                    log.info("Scheduled Re-Assign on Shard({}).", currentAssignment);
                }
                return null;
            });
        });
    }

    public CompletableFuture<Void> consumerNodeJoined(ConsumerNode consumerNode) {
        return getConsumerInfo(consumerNode.getConsumerId()).thenCompose(ci -> {
            consumerNode.initFromConsumerInfo(ci);
            return assignmentManager.consumerNodeJoined(consumerNode);
        });
    }

    private CompletableFuture<ConsumerInfo> getConsumerInfo(String consumerId) {
        ConsumerApi consumer = consumerClientFactory.getInstance(consumerId);
        return consumer.getConsumerInfo();
    }

    public List<Assignment> getAllAssignments() {
        return assignmentManager.getAllAssignments();
    }

    public List<SubscriptionOperation> getPendingSubOps() {
        return operationMgr.getPendingSubOps();
    }

    public void retryOperation(SubscriptionOperation operation) {
        VaradhiSubscription subscription = metaStore.getSubscription(operation.getData().getSubscriptionId());
        OpExecutor<OrderedOperation> executor = getOpExecutor(operation, subscription);
        operationMgr.enqueue(operation, executor);
    }

    private OpExecutor<OrderedOperation> getOpExecutor(
            SubscriptionOperation operation, VaradhiSubscription subscription
    ) {
        //TODO::Better handling needed
        if (operation.getData() instanceof SubscriptionOperation.StartData) {
            return new StartOpExecutor(subscription);
        } else if (operation.getData() instanceof SubscriptionOperation.StopData) {
            return new StopOpExecutor(subscription);
        } else if (operation.getData() instanceof SubscriptionOperation.ReassignShardData) {
            return new ReAssignOpExecutor();
        } else {
            throw new IllegalArgumentException("Can't get OpExecutor for Operation %s.".formatted(operation.getData()));
        }
    }

    class StartOpExecutor implements OpExecutor<OrderedOperation> {
        VaradhiSubscription subscription;

        StartOpExecutor(VaradhiSubscription subscription) {
            this.subscription = subscription;
        }

        @Override
        public CompletableFuture<Void> execute(OrderedOperation operation) {
            return startShards((SubscriptionOperation) operation, subscription);
        }
    }

    class StopOpExecutor implements OpExecutor<OrderedOperation> {
        VaradhiSubscription subscription;

        StopOpExecutor(VaradhiSubscription subscription) {
            this.subscription = subscription;
        }

        @Override
        public CompletableFuture<Void> execute(OrderedOperation operation) {
            return stopShards((SubscriptionOperation) operation, subscription);
        }
    }

    class ReAssignOpExecutor implements OpExecutor<OrderedOperation> {
        @Override
        public CompletableFuture<Void> execute(OrderedOperation operation) {
            return reAssignShard((SubscriptionOperation) operation);
        }
    }
}
