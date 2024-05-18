package com.flipkart.varadhi.controller;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.flipkart.varadhi.entities.cluster.Assignment;
import com.flipkart.varadhi.entities.cluster.ConsumerNode;
import com.flipkart.varadhi.core.cluster.*;
import com.flipkart.varadhi.entities.SubscriptionShards;
import com.flipkart.varadhi.entities.SubscriptionUnitShard;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.db.MetaStoreProvider;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class ControllerApiMgr implements ControllerApi {
    private final WebServerApi webServerApiProxy;
    private final ShardAssigner shardAssigner;
    private final ConsumerClientFactory consumerClientFactory;
    private final MetaStore metaStore;
    private final OperationMgr operationMgr;

    public ControllerApiMgr(
            WebServerApi webServerApiProxy, ConsumerClientFactory consumerClientFactory, MetaStoreProvider metaStoreProvider
    ) {
        this.webServerApiProxy = webServerApiProxy;
        this.consumerClientFactory = consumerClientFactory;
        this.shardAssigner = new ShardAssigner(metaStoreProvider.getAssignmentStore());
        this.metaStore = metaStoreProvider.getMetaStore();
        this.operationMgr = new OperationMgr(metaStoreProvider.getOpStore());
    }

    public void addConsumerNodes(List<ConsumerNode> clusterConsumers) {
        shardAssigner.addConsumerNodes(clusterConsumers);
    }

    /**
     * Start the subscription
     * TODO::It makes single attempt to start the Subscription, on failure Operation is marked as failed and no
     * retry is attempted. Evaluate how retry should be done (may be in operation mgr)
     */

    @Override
    public CompletableFuture<Void> startSubscription(SubscriptionOperation.StartData opData) {
        log.info("Starting the Subscription: {}", opData);

        if (opData.completed()) {
            log.warn("Ignoring already Subscription Operation: {}", opData);
            return CompletableFuture.completedFuture(null);
        }

        opData.markInProgress();
        return webServerApiProxy.update(opData).thenAccept(v -> {
            String subId = opData.getSubscriptionId();
            VaradhiSubscription subscription = metaStore.getSubscription(subId);
            List<Assignment> assignedShards = shardAssigner.getSubscriptionAssignment(subscription.getName());
            if (assignedShards.isEmpty()) {
                List<SubscriptionUnitShard> unAssigned = getSubscriptionShards(subscription.getShards());
                assignedShards.addAll(shardAssigner.assignShard(unAssigned, subscription));
            } else {
                log.info(
                        "{} Shards for Subscription {} are already assigned", assignedShards.size(),
                        subscription.getName()
                );
            }

            SubscriptionShards shards = subscription.getShards();
            CompletableFuture.allOf(assignedShards.stream()
                    .map(assignment -> startShard(assignment, shards.getShard(assignment.getShardId()), subscription))
                    .toArray(CompletableFuture[]::new)).exceptionally(t -> {
                markSubOpFailed(opData, t);
                return null;
            });
            log.info("Scheduled Subscription start({}).", opData);
        }).exceptionally(t -> {
            markSubOpFailed(opData, t);
            return null;
        });
    }


    private void markSubOpFailed(SubscriptionOperation.OpData opData, Throwable t) {
        log.error("Failed to schedule Subscription start({}): {}.", opData, t);
        opData.markFail(t.getMessage());
        webServerApiProxy.update(opData);
    }


    /**
     * Start the shard if it is not already started.
     * TODO:: Implement queuing/sequencing of the operations for specific shard (may be in operations mgr)
     */
    private CompletableFuture<Void> startShard(
            Assignment assignment, SubscriptionUnitShard shard, VaradhiSubscription subscription
    ) {
        ConsumerApi consumer = getAssignedConsumer(assignment);
        return consumer.getStatus(subscription.getName(), shard.getShardId()).thenAccept(shardStatus -> {
            if (!shardStatus.isAssigned()) {
                ShardOperation.StartData opData = operationMgr.requestShardStart(shard, subscription);
                consumer.start(opData).whenComplete((v, t) -> {
                    if (t != null) {
                        markShardOpFailed(opData, t);
                    }else{
                        log.info("Scheduled shard start({}).", opData);
                    }
                });
            }
        });
    }
    private void markShardOpFailed(ShardOperation.OpData opData, Throwable t) {
        log.error("Failed to schedule shard start({}): {}.", opData, t.getMessage());
        opData.markFail(t.getMessage());
        update(opData);
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
    public CompletableFuture<Void> stopSubscription(SubscriptionOperation.StopData operation) {
        operation.markInProgress();
        return webServerApiProxy.update(operation);
    }

    @Override
    public CompletableFuture<Void> update(ShardOperation.OpData opData) {
        log.debug("Received update on shard operation: {}", opData);
        operationMgr.updateShardOp(opData);
        return CompletableFuture.completedFuture(null);
    }

    public void consumerNodeLeft(String consumerNodeId) {
        shardAssigner.consumerNodeLeft(consumerNodeId);
    }

    public void consumerNodeJoined(ConsumerNode consumerNode) {
        shardAssigner.consumerNodeJoined(consumerNode);
    }
}
