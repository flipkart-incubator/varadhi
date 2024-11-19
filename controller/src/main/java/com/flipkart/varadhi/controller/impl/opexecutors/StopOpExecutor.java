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
public class StopOpExecutor extends SubscriptionOpExecutor {
    public StopOpExecutor(
            VaradhiSubscription subscription, ConsumerClientFactory clientFactory, OperationMgr operationMgr,
            AssignmentManager assignmentManager, MetaStore metaStore
    ) {
        super(subscription, clientFactory, operationMgr, assignmentManager, metaStore);
    }

    @Override
    public CompletableFuture<Void> execute(OrderedOperation operation) {
        return stopShards((SubscriptionOperation) operation, subscription);
    }

    private CompletableFuture<Void> stopShards(SubscriptionOperation subOp, VaradhiSubscription subscription) {
        String subId = subscription.getName();
        SubscriptionShards shards = subscription.getShards();
        List<Assignment> assignments = assignmentManager.getSubAssignments(subId);
        log.info(
                "Found {} assigned Shards for the Subscription:{} with total {} Shards.", assignments.size(), subId,
                shards.getShardCount()
        );

        List<Assignment> stopsFailed = new ArrayList<>();
        List<CompletableFuture<Boolean>> shardFutures =
                scheduleStopOnShards(subscription, subOp, assignments, stopsFailed);
        log.info(
                "Executed Stop on {} shards for SubOp({}), Scheduled ShardOperations {}.", shards.getShardCount(),
                subOp.getData(), shardFutures.size()
        );

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
                if (shardOp.hasFailed()) {
                    stopsFailed.add(assignment);
                }
            });
        }).toList();
    }


    private CompletableFuture<Boolean> stopShard(ShardOperation stopOp, boolean isRetry, ConsumerApi consumer) {
        String subId = stopOp.getOpData().getSubscriptionId();
        int shardId = stopOp.getOpData().getShardId();
        return consumer.getConsumerState(subId, shardId).thenCompose(state -> {
            // Stop can be executed in starting subscription as well.
            // in general this shouldn't happen as multiple in-progress operations are not allowed.
            if (state.isEmpty()) {
                log.info("Subscription:{} Shard:{} is already Stopped. Skipping.", subId, shardId);
                return CompletableFuture.completedFuture(false);
            }
            operationMgr.submitShardOp(stopOp, isRetry);
            CompletableFuture<Void> stopFuture = consumer.stop((ShardOperation.StopData) stopOp.getOpData());
            log.info("Scheduled shard stop({}).", stopOp);
            return stopFuture.thenApply(v -> true);
        }).exceptionally(t -> {
            failShardOperation(stopOp, t);
            return true;
        });
    }
}
