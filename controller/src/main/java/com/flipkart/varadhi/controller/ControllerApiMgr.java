package com.flipkart.varadhi.controller;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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

    public CompletableFuture<Void> addConsumerNodes(List<ConsumerNode> clusterConsumers) {
        return CompletableFuture.allOf(clusterConsumers.stream()
                        .map(cc -> getConsumerInfo(cc.getConsumerId()).thenAccept(
                                cc::initFromConsumerInfo)).toArray(CompletableFuture[]::new))
                .thenCompose(v -> shardAssigner.addConsumerNodes(clusterConsumers));
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
            // operationMgr is not expected to create a subOp and throw, so failure is not handled here.
            return operationMgr.requestSubStart(
                    subscriptionId, requestedBy, subOp -> getOrCreateShardAssignment(subscription).thenCompose(
                            assignments -> startShards((SubscriptionOperation) subOp, subscription, assignments)));
        });
    }

    private CompletableFuture<List<Assignment>> getOrCreateShardAssignment(VaradhiSubscription subscription) {
        List<Assignment> assignedShards = shardAssigner.getSubscriptionAssignment(subscription.getName());
        if (assignedShards.isEmpty()) {
            List<SubscriptionUnitShard> unAssigned = getSubscriptionShards(subscription.getShards());
            return shardAssigner.assignShard(unAssigned, subscription, new HashSet<>());
        } else {
            log.info(
                    "{} Shards for Subscription {} are already assigned.", assignedShards.size(),
                    subscription.getName()
            );
            return CompletableFuture.completedFuture(assignedShards);
        }
    }

    private CompletableFuture<Void> startShards(
            SubscriptionOperation subOp, VaradhiSubscription subscription, List<Assignment> assignments
    ) {
        SubscriptionShards shards = subscription.getShards();
        String subOpId = subOp.getData().getOperationId();
        CompletableFuture<Void> future = CompletableFuture.allOf(assignments.stream()
                .map(assignment -> startShard(subOpId, assignment, shards.getShard(assignment.getShardId()),
                        subscription
                )).toArray(CompletableFuture[]::new)).exceptionally(t -> {
            markSubOpFailed(subOp, t);
            return null;
        });
        log.info("Scheduled Start on {} shards for SubOp({}).", shards.getShardCount(), subOp.getData());
        return future;
    }

    private CompletableFuture<Void> startShard(
            String subOpId, Assignment assignment, SubscriptionUnitShard shard, VaradhiSubscription subscription
    ) {
        String subId = subscription.getName();
        int shardId = shard.getShardId();
        ConsumerApi consumer = getAssignedConsumer(assignment);
        return consumer.getShardStatus(subId, shardId).thenCompose(shardStatus -> {
            // IsAssigned is started|starting|errored.
            // Stopping isn't considered as assigned, as start/stop shouldn't be running in parallel.
            if (!shardStatus.isAssigned()) {
                ShardOperation shardOp = operationMgr.requestShardStart(subOpId, shard, subscription);
                return consumer.start((ShardOperation.StartData) shardOp.getOpData()).whenComplete((v, t) -> {
                    if (t != null) {
                        markShardOpFailed(shardOp, t);
                    } else {
                        log.info("Scheduled shard start({}).", shardOp);
                    }
                });
            } else {
                log.info("Subscription:{} Shard:{} is already started. Skipping.", subId, shardId);
                return CompletableFuture.completedFuture(null);
            }
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
            // operationMgr is not expected to create a subOp and throw, so failure is not handled here.
            return operationMgr.requestSubStop(
                    subscriptionId, requestedBy, subOp -> stopShards((SubscriptionOperation) subOp, subscription));
        });
    }

    private CompletableFuture<Void> stopShards(SubscriptionOperation subOp, VaradhiSubscription subscription) {
        String subId = subscription.getName();
        String subOpId = subOp.getData().getOperationId();
        SubscriptionShards shards = subscription.getShards();
        List<Assignment> assignments = shardAssigner.getSubscriptionAssignment(subId);
        log.info(
                "Found {} assigned Shards for the Subscription:{} with Shards:{}.", assignments.size(),
                subId, shards.getShardCount()
        );
        CompletableFuture<Void> future = CompletableFuture.allOf(assignments.stream()
                .map(assignment -> stopShard(subOpId, assignment, shards.getShard(assignment.getShardId()),
                        subscription
                )).toArray(CompletableFuture[]::new)).exceptionally(t -> {
            markSubOpFailed(subOp, t);
            return null;
        }).thenCompose(v -> shardAssigner.unAssignShard(assignments, subscription, true));
        log.info("Scheduled Stop on {} shards for SubOp({}).", shards.getShardCount(), subOp.getData());
        return future;
    }

    private CompletableFuture<Void> stopShard(
            String subOpId, Assignment assignment, SubscriptionUnitShard shard, VaradhiSubscription subscription
    ) {
        String subId = subscription.getName();
        int shardId = shard.getShardId();
        ConsumerApi consumer = getAssignedConsumer(assignment);
        return consumer.getShardStatus(subId, shardId).thenCompose(shardStatus -> {
            if (!shardStatus.isAssigned()) {
                ShardOperation shardOp = operationMgr.requestShardStop(subOpId, shard, subscription);
                return consumer.stop((ShardOperation.StopData) shardOp.getOpData()).whenComplete((v, t) -> {
                    if (t != null) {
                        markShardOpFailed(shardOp, t);
                    } else {
                        log.info("Scheduled shard stop({}).", shardOp);
                    }
                });
            } else {
                log.info("Subscription:{} Shard:{} is already Stopped. Skipping.", subId, shardId);
                return CompletableFuture.completedFuture(null);
            }
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
                operationMgr.requestShardReassign(
                        assignment, SYSTEM_IDENTITY, subOp -> reAssignShard((SubscriptionOperation) subOp));
            });
        });
    }

    private CompletableFuture<Void> reAssignShard(SubscriptionOperation subOp) {
        //TODO:: failure recovery of re-assign needs to be taken care of.
        SubscriptionOperation.ReassignShardData data = (SubscriptionOperation.ReassignShardData) subOp.getData();
        Assignment currentAssignment = data.getAssignment();
        VaradhiSubscription subscription = metaStore.getSubscription(currentAssignment.getSubscriptionId());
        SubscriptionUnitShard shard = subscription.getShards().getShard(currentAssignment.getShardId());
        return shardAssigner.reAssignShard(currentAssignment, subscription, false).thenCompose(a ->
                startShard(subOp.getId(), a, shard, subscription));
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
}
