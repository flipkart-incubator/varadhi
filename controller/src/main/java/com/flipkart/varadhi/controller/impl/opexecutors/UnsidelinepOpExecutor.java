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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class UnsidelinepOpExecutor extends SubscriptionOpExecutor {
    public UnsidelinepOpExecutor(
            VaradhiSubscription subscription, ConsumerClientFactory clientFactory, OperationMgr operationMgr,
            AssignmentManager assignmentManager, MetaStore metaStore
    ) {
        super(subscription, clientFactory, operationMgr, assignmentManager, metaStore);
    }

    @Override
    public CompletableFuture<Void> execute(OrderedOperation operation) {
        return unsidelineShards((SubscriptionOperation) operation, subscription);
    }

    private CompletableFuture<Void> unsidelineShards(
            SubscriptionOperation subOp, VaradhiSubscription subscription
    ) {
        String subId = subscription.getName();
        SubscriptionShards shards = subscription.getShards();
        List<Assignment> assignments = assignmentManager.getSubAssignments(subId);
        log.info(
                "Found {} assigned Shards for the Subscription:{} with total {} Shards.", assignments.size(), subId,
                shards.getShardCount()
        );

        List<CompletableFuture<Boolean>> shardFutures =
                scheduleUnsidelineOnShards(subscription, subOp, assignments);
        log.info(
                "Executed Un-sideline on {} shards for SubOp({}), Scheduled ShardOperations {}.",
                shards.getShardCount(), subOp.getData(), shardFutures.size()
        );

        return CompletableFuture.allOf(shardFutures.toArray(new CompletableFuture[0])).thenApply(ignore -> {
            if (allShardsSkipped(shardFutures)) {
                log.info("Un-sideline {} completed without any shard operations being scheduled.", subOp.getData());
                failSubOperation(subOp, new IllegalStateException("Unsideline could not be scheduled now."));
            }
            return null;
        });
    }

    private List<CompletableFuture<Boolean>> scheduleUnsidelineOnShards(
            VaradhiSubscription subscription, SubscriptionOperation subOp, List<Assignment> assignments
    ) {
        SubscriptionShards shards = subscription.getShards();
        SubscriptionOperation.UnsidelineData opData = (SubscriptionOperation.UnsidelineData) subOp.getData();
        String subOpId = opData.getOperationId();
        Map<Integer, ShardOperation> shardOps = operationMgr.getShardOps(subOpId);
        return assignments.stream().map(assignment -> {
            ConsumerApi consumer = getAssignedConsumer(assignment);
            SubscriptionUnitShard shard = shards.getShard(assignment.getShardId());
            ShardOperation shardOp = shardOps.computeIfAbsent(
                    assignment.getShardId(),
                    shardId -> ShardOperation.unsidelineOp(subOpId, shard, subscription, opData.getRequest())
            );
            return unsidelineShard(shardOp, subOp.isInRetry(), consumer);
        }).toList();
    }

    private CompletableFuture<Boolean> unsidelineShard(
            ShardOperation unsidelineOp, boolean isRetry, ConsumerApi consumer
    ) {
        ShardOperation.UnsidelineData opData = (ShardOperation.UnsidelineData) unsidelineOp.getOpData();
        String subId = opData.getSubscriptionId();
        int shardId = opData.getShardId();
        // TOOD: why is state check even required? why not assume "happy path" and then handle errors?
        return consumer.getConsumerState(subId, shardId).thenCompose(state -> {
            // unsideline can be executed when subscription is in running/started state.
            // may be special handing needs to be enabled when only DLQ unsideline needs to be handled.
            if (state.isEmpty()) {
                log.error("Subscription:{} Shard:{} is not in started state, can't un-sideline.", subId, shardId);
                //TODO::evaluate sending appropriate error ??
                return CompletableFuture.completedFuture(false);
            }
            operationMgr.submitShardOp(unsidelineOp, isRetry);
            CompletableFuture<Void> future = consumer.unsideline(opData);
            log.info("Scheduled shard un-sideline({}).", opData);
            return future.thenApply(v -> true);
        }).exceptionally(t -> {
            failShardOperation(unsidelineOp, t);
            return true;
        });
    }
}
