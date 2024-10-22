package com.flipkart.varadhi.controller.impl.opexecutors;

import com.flipkart.varadhi.controller.AssignmentManager;
import com.flipkart.varadhi.controller.OperationMgr;
import com.flipkart.varadhi.core.cluster.ConsumerApi;
import com.flipkart.varadhi.core.cluster.ConsumerClientFactory;
import com.flipkart.varadhi.entities.SubscriptionShards;
import com.flipkart.varadhi.entities.SubscriptionUnitShard;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.cluster.Assignment;
import com.flipkart.varadhi.entities.cluster.OrderedOperation;
import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.spi.db.MetaStore;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class StartOpExecutor extends SubscriptionStartShardExecutor {
    public StartOpExecutor(
            VaradhiSubscription subscription, ConsumerClientFactory clientFactory, OperationMgr operationMgr,
            AssignmentManager assignmentManager, MetaStore metaStore
    ) {
        super(subscription, clientFactory, operationMgr, assignmentManager, metaStore);
    }

    @Override
    public CompletableFuture<Void> execute(OrderedOperation operation) {
        return startShards((SubscriptionOperation) operation, subscription);
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

    private List<SubscriptionUnitShard> getSubscriptionShards(SubscriptionShards shards) {
        List<SubscriptionUnitShard> unitShards = new ArrayList<>();
        for (int shardId = 0; shardId < shards.getShardCount(); shardId++) {
            SubscriptionUnitShard shard = shards.getShard(shardId);
            unitShards.add(shard);
        }
        return unitShards;
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
}
