package com.flipkart.varadhi.controller.impl.opexecutors;

import com.flipkart.varadhi.controller.AssignmentManager;
import com.flipkart.varadhi.controller.OperationMgr;
import com.flipkart.varadhi.core.cluster.ConsumerApi;
import com.flipkart.varadhi.core.cluster.ConsumerClientFactory;
import com.flipkart.varadhi.entities.SubscriptionUnitShard;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.cluster.Assignment;
import com.flipkart.varadhi.entities.cluster.OrderedOperation;
import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.spi.db.subscription.SubscriptionMetaStore;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class ReAssignOpExecutor extends SubscriptionStartShardExecutor {

    public ReAssignOpExecutor(
        VaradhiSubscription subscription,
        ConsumerClientFactory clientFactory,
        OperationMgr operationMgr,
        AssignmentManager assignmentManager,
        SubscriptionMetaStore metaStore
    ) {
        super(subscription, clientFactory, operationMgr, assignmentManager, metaStore);
    }


    @Override
    public CompletableFuture<Void> execute(OrderedOperation operation) {
        return reAssignShard((SubscriptionOperation)operation);
    }

    /**
     * Re-Assigns shard to a Consumer Node different from current assignment.
     * Re-Assign = UnAssign -> Assign.
     * If there is a failure during Assign, Subscription shard remains un-assigned.
     * -- Retry (Auto or manual) of failed Re-Assign operation or start of the subscription should fix this.
     */
    private CompletableFuture<Void> reAssignShard(SubscriptionOperation subOp) {
        SubscriptionOperation.ReassignShardData data = (SubscriptionOperation.ReassignShardData)subOp.getData();
        Assignment currentAssignment = data.getAssignment();
        VaradhiSubscription subscription = subscriptionMetaStore.getSubscription(currentAssignment.getSubscriptionId());
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
}
