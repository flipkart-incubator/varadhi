package com.flipkart.varadhi.controller;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.flipkart.varadhi.controller.config.ControllerConfig;
import com.flipkart.varadhi.entities.cluster.*;
import com.flipkart.varadhi.core.cluster.*;
import com.flipkart.varadhi.entities.SubscriptionShards;
import com.flipkart.varadhi.entities.SubscriptionUnitShard;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.db.MetaStoreProvider;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

import static com.flipkart.varadhi.Constants.SYSTEM_IDENTITY;

@Slf4j
public class ControllerApiMgr implements ControllerApi {
    private final ShardAssigner shardAssigner;
    private final ConsumerClientFactory consumerClientFactory;
    private final MetaStore metaStore;
    private final OperationMgr operationMgr;

    public ControllerApiMgr(
            ControllerConfig config, ConsumerClientFactory consumerClientFactory, MetaStoreProvider metaStoreProvider,
            MeterRegistry meterRegistry
    ) {
        this.consumerClientFactory = consumerClientFactory;
        this.shardAssigner = new ShardAssigner(metaStoreProvider.getAssignmentStore(), meterRegistry);
        this.metaStore = metaStoreProvider.getMetaStore();
        this.operationMgr = new OperationMgr(config, metaStoreProvider.getOpStore());
    }

    public CompletableFuture<String> addConsumerNode(ConsumerNode consumerNode) {
        return getConsumerInfo(consumerNode.getConsumerId()).thenApply(ci -> {
            consumerNode.initFromConsumerInfo(ci);
            shardAssigner.addConsumerNode(consumerNode);
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

    private CompletableFuture<SubscriptionStatus> getSubscriptionStatus(VaradhiSubscription subscription) {
        String subId = subscription.getName();
        List<Assignment> assignments = shardAssigner.getSubscriptionAssignment(subId);

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


    /**
     * Start the subscription
     * TODO::It makes single attempt to start the Subscription, on failure Operation is marked as failed and no
     * retry is attempted. Evaluate how retry should be done (may be in operation mgr)
     */

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
                throw new InvalidOperationForResourceException("Subscription is already running or starting.");
            }
            log.info("Starting the Subscription: {}", subscriptionId);
            SubscriptionOperation operation = SubscriptionOperation.startOp(subscriptionId, requestedBy);
            operationMgr.createAndEnqueue(operation, new StartOpExecutor(subscription));
            return operation;
        });
    }

    private CompletableFuture<Void> startShards(SubscriptionOperation subOp, VaradhiSubscription subscription) {
        SubscriptionShards shards = subscription.getShards();
        String subOpId = subOp.getData().getOperationId();
        return getOrCreateShardAssignment(subscription).thenCompose(assignments -> {

            Map<Integer, ShardOperation> shardOps = operationMgr.getShardOps(subOpId);
            CompletableFuture<Void> future = CompletableFuture.allOf(assignments.stream().map(assignment -> {
                ConsumerApi consumer = getAssignedConsumer(assignment);
                SubscriptionUnitShard shard = shards.getShard(assignment.getShardId());
                ShardOperation shardOp = shardOps.computeIfAbsent(
                        assignment.getShardId(),
                        shardId -> ShardOperation.startOp(subOpId, shard, subscription)
                );
                return startShard(shardOp, consumer);
            }).toArray(CompletableFuture[]::new)).exceptionally(t -> {
                markSubOpFailed(subOp, t);
                return null;
            });
            log.info("Scheduled Start on {} shards for SubOp({}).", shards.getShardCount(), subOp.getData());
            return future;
        });
    }

    private CompletableFuture<List<Assignment>> getOrCreateShardAssignment(VaradhiSubscription subscription) {
        List<Assignment> assignedShards = shardAssigner.getSubscriptionAssignment(subscription.getName());
        if (assignedShards.isEmpty()) {
            List<SubscriptionUnitShard> unAssigned = getSubscriptionShards(subscription.getShards());
            return shardAssigner.assignShards(unAssigned, subscription, new HashSet<>());
        } else {
            log.info(
                    "{} Shards for Subscription {} are already assigned.", assignedShards.size(),
                    subscription.getName()
            );
            return CompletableFuture.completedFuture(assignedShards);
        }
    }

    private CompletableFuture<Void> startShard(ShardOperation startOp, ConsumerApi consumer) {
        String subId = startOp.getOpData().getSubscriptionId();
        int shardId = startOp.getOpData().getShardId();
        return consumer.getShardStatus(subId, shardId).thenCompose(shardStatus -> {
            // IsAssigned is started|starting|errored.
            // Stopping isn't considered as assigned, as start/stop shouldn't be running in parallel.
            if (!shardStatus.isAssigned()) {
                return operationMgr.createIfNeededAndExecute(startOp, op -> {
                    ShardOperation shardOp = (ShardOperation) op;
                    return consumer.start((ShardOperation.StartData) shardOp.getOpData()).whenComplete((v, t) -> {
                        if (t != null) {
                            markShardOpFailed(shardOp, t);
                        } else {
                            log.info("Scheduled shard start({}).", shardOp);
                        }
                    });
                });
            } else {
                log.info("Subscription:{} Shard:{} is already started. Skipping.", subId, shardId);
                return CompletableFuture.completedFuture(null);
            }
        }).exceptionally(t -> {
            log.error("Subscription:{} Shard:{} failed to start {}", subId, shardId, t.getMessage());
            markShardOpFailed(startOp, t);
            return null;
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
            if (ss.getState() == SubscriptionState.STOPPED) {
                throw new InvalidOperationForResourceException("Subscription is already stopped.");
            }
            log.info("Stopping the Subscription: {}", subscriptionId);
            SubscriptionOperation operation = SubscriptionOperation.stopOp(subscriptionId, requestedBy);
            operationMgr.createAndEnqueue(operation, new StopOpExecutor(subscription));
            return operation;
        });
    }

    private CompletableFuture<Void> stopShards(SubscriptionOperation subOp, VaradhiSubscription subscription) {
        String subId = subscription.getName();
        String subOpId = subOp.getData().getOperationId();
        SubscriptionShards shards = subscription.getShards();
        List<Assignment> assignments = shardAssigner.getSubscriptionAssignment(subId);
        log.info(
                "Found {} assigned Shards for the Subscription:{} with total {} Shards.", assignments.size(),
                subId, shards.getShardCount()
        );
        Map<Integer, ShardOperation> shardOps = operationMgr.getShardOps(subOpId);
        CompletableFuture<Void> future = CompletableFuture.allOf(assignments.stream().map(assignment -> {
            ConsumerApi consumer = getAssignedConsumer(assignment);
            SubscriptionUnitShard shard = shards.getShard(assignment.getShardId());
            ShardOperation shardOp = shardOps.computeIfAbsent(
                    assignment.getShardId(),
                    shardId -> ShardOperation.stopOp(subOpId, shard, subscription)
            );
            return stopShard(shardOp, consumer);
        }).toArray(CompletableFuture[]::new)).exceptionally(t -> {
            markSubOpFailed(subOp, t);
            return null;
        }).thenCompose(v -> shardAssigner.unAssignShards(assignments, subscription, true));
        log.info("Scheduled Stop on {} shards for SubOp({}).", shards.getShardCount(), subOp.getData());
        return future;
    }

    private CompletableFuture<Void> stopShard(ShardOperation stopOp, ConsumerApi consumer) {
        String subId = stopOp.getOpData().getSubscriptionId();
        int shardId = stopOp.getOpData().getShardId();
        return consumer.getShardStatus(subId, shardId).thenCompose(shardStatus -> {
            if (!shardStatus.isAssigned()) {
                return operationMgr.createIfNeededAndExecute(stopOp, op -> {
                    ShardOperation shardOp = (ShardOperation) op;
                    return consumer.stop((ShardOperation.StopData) shardOp.getOpData()).whenComplete((v, t) -> {
                        if (t != null) {
                            markShardOpFailed(shardOp, t);
                        } else {
                            log.info("Scheduled shard stop({}).", shardOp);
                        }
                    });
                });
            } else {
                log.info("Subscription:{} Shard:{} is already Stopped. Skipping.", subId, shardId);
                return CompletableFuture.completedFuture(null);
            }
        }).exceptionally(t -> {
            log.error("Subscription:{} Shard:{} failed to stop {}", subId, shardId, t.getMessage());
            markShardOpFailed(stopOp, t);
            return null;
        });
    }

    private void markSubOpFailed(SubscriptionOperation subscriptionOp, Throwable t) {
        log.error("Subscription operation ({}) failed: {}.", subscriptionOp, t);
        subscriptionOp.markFail(t.getMessage());
        operationMgr.updateSubOp(subscriptionOp);
    }

    private void markShardOpFailed(ShardOperation shardOp, Throwable t) {
        log.error("shard operation ({}) failed: {}.", shardOp, t.getMessage());
        shardOp.markFail(t.getMessage());
        operationMgr.updateShardOp(shardOp.getOpData());
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
    public CompletableFuture<Void> update(ShardOperation.OpData opData) {
        log.info("Received update on shard operation: {}", opData);
        try {
            // Update is getting executed inline on dispatcher thread.
            operationMgr.updateShardOp(opData);
            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    public CompletableFuture<Void> consumerNodeLeft(String consumerNodeId) {
        log.info("ConsumerNode {} left the cluster.", consumerNodeId);
        return shardAssigner.consumerNodeLeft(consumerNodeId).thenAccept((v) -> {
            List<Assignment> assignments = shardAssigner.getConsumerNodeAssignment(consumerNodeId);
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
     *
     * @param subOp
     *
     * @return
     */
    private CompletableFuture<Void> reAssignShard(SubscriptionOperation subOp) {
        //TODO:: failure recovery of re-assign needs to be taken care of.
        SubscriptionOperation.ReassignShardData data = (SubscriptionOperation.ReassignShardData) subOp.getData();
        Assignment currentAssignment = data.getAssignment();
        VaradhiSubscription subscription = metaStore.getSubscription(currentAssignment.getSubscriptionId());
        Map<Integer, ShardOperation> shardOps = operationMgr.getShardOps(subOp.getId());

        return shardAssigner.reAssignShard(currentAssignment, subscription, false).thenCompose(a -> {
            ConsumerApi consumer = getAssignedConsumer(a);
            SubscriptionUnitShard shard = subscription.getShards().getShard(currentAssignment.getShardId());
            ShardOperation shardOp = shardOps.computeIfAbsent(
                    a.getShardId(),
                    shardId -> ShardOperation.startOp(subOp.getId(), shard, subscription)
            );
            return startShard(shardOp, consumer);
        });
    }

    public CompletableFuture<Void> consumerNodeJoined(ConsumerNode consumerNode) {
        return getConsumerInfo(consumerNode.getConsumerId()).thenCompose(ci -> {
            consumerNode.initFromConsumerInfo(ci);
            return shardAssigner.consumerNodeJoined(consumerNode);
        });
    }

    private CompletableFuture<ConsumerInfo> getConsumerInfo(String consumerId) {
        ConsumerApi consumer = consumerClientFactory.getInstance(consumerId);
        return consumer.getConsumerInfo();
    }

    public List<Assignment> getAllAssignments() {
        return shardAssigner.getAllAssignments();
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
