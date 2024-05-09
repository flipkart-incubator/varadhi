package com.flipkart.varadhi.controller;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.flipkart.varadhi.controller.entities.Assignment;
import com.flipkart.varadhi.controller.entities.ConsumerNode;
import com.flipkart.varadhi.core.cluster.*;
import com.flipkart.varadhi.entities.SubscriptionShards;
import com.flipkart.varadhi.entities.SubscriptionUnitShard;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.spi.db.MetaStore;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class ControllerApiMgr implements ControllerApi {
    private final WebServerApi webServerApiProxy;
    private final ShardAssigner shardAssigner;
    private final ConsumerApiFactory consumerApiFactory;
    private final MetaStore metaStore;

    public ControllerApiMgr(WebServerApi webServerApiProxy, ConsumerApiFactory consumerApiFactory, MetaStore metaStore) {
        this.webServerApiProxy = webServerApiProxy;
        this.consumerApiFactory = consumerApiFactory;
        this.shardAssigner = new ShardAssigner();
        this.metaStore = metaStore;
    }

    public void addConsumerNodes(List<ConsumerNode> clusterConsumers) {
        shardAssigner.addConsumerNodes(clusterConsumers);
    }

    @Override
    public CompletableFuture<Void> startSubscription(SubscriptionOperation.StartData operation) {
        log.info("Starting the Subscription: {}", operation);
        if (operation.completed()) {
            log.warn("Ignoring already finished Operation: {}", operation);
            return CompletableFuture.completedFuture(null);
        }

        operation.markInProgress();
        return webServerApiProxy.update(operation).thenAccept(v -> {

            String subId = operation.getSubscriptionId();
            VaradhiSubscription subscription = metaStore.getSubscription(subId);
            List<SubscriptionUnitShard> unAssigned = getUnAssignedShards(subscription);
            List<Assignment> assignments = shardAssigner.assignShard(unAssigned, subscription);
            SubscriptionShards shards = subscription.getShards();
            log.info("Obtained shard: {}", shards);
            for (Assignment assignment : assignments) {
                int shardId = assignment.getShardId();
                SubscriptionUnitShard shard = shards.getShard(shardId);
                String consumerId = assignment.getConsumerId();
                //TODO:: check for any operation already queued on Shard -- since these are unassigned does it matter ??
                ShardOperation.StartData startOp = new ShardOperation.StartData(shardId, shard, subscription);
                //TODO::handle exception to send the request.
                //TODO:: check if Send should be kind of sync operation ??
                consumerApiFactory.getConsumerProxy(consumerId).start(startOp);
            }
            // TODO:: handle already assigned shard as well.
            log.info("Completed the scheduling {}.", operation);

        }).exceptionally(t -> {
            log.error("Failed to start", t);
            operation.markFail(t.getMessage());
            webServerApiProxy.update(operation);
            return null;
        });
    }

    private List<SubscriptionUnitShard> getUnAssignedShards(VaradhiSubscription subscription) {
        SubscriptionShards shards = subscription.getShards();
        Map<Integer, Assignment> existingAssignments = shardAssigner.getShardsAssignment(shards, subscription).stream()
                .collect(Collectors.toMap(Assignment::getShardId, a -> a));

        List<SubscriptionUnitShard> unAssigned = new ArrayList<>();
        for (int shardId = 0; shardId < subscription.getShards().getShardCount(); shardId++) {
            SubscriptionUnitShard shard = shards.getShard(shardId);
            Assignment assignment = existingAssignments.getOrDefault(shardId, null);
            if (null != assignment) {
                log.info("Shard {} already assigned {}.", shard, assignment);
                continue;
            }
            unAssigned.add(shard);
        }
        return unAssigned;
    }

    @Override
    public CompletableFuture<Void> stopSubscription(SubscriptionOperation.StopData operation) {
        operation.markInProgress();
        return webServerApiProxy.update(operation);
    }

    @Override
    public CompletableFuture<Void> update(ShardOperation.OpData operation) {
        log.debug("Received update on shard operation: {}", operation);
        return CompletableFuture.completedFuture(null);
    }

    public void consumerNodeLeft(String consumerNodeId) {
        shardAssigner.consumerNodeLeft(consumerNodeId);
    }

    public void consumerNodeJoined(ConsumerNode consumerNode) {
        shardAssigner.consumerNodeJoined(consumerNode);
    }
}
