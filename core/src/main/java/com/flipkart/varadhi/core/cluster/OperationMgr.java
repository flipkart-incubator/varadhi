package com.flipkart.varadhi.core.cluster;

import com.flipkart.varadhi.entities.SubscriptionUnitShard;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.spi.db.OpStore;

public class OperationMgr {
    private final OpStore opStore;

    public OperationMgr(OpStore opStore) {
        this.opStore = opStore;
    }

    public SubscriptionOperation requestSubStart(String subscriptionId, String requestedBy) {
        SubscriptionOperation operation = SubscriptionOperation.startOp(subscriptionId, requestedBy);
        opStore.createSubOp(operation);
        return operation;
    }

    public SubscriptionOperation requestSubStop(String subscriptionId, String requestedBy) {
        SubscriptionOperation operation = SubscriptionOperation.stopOp(subscriptionId, requestedBy);
        opStore.createSubOp(operation);
        return operation;
    }

    public void updateSubOp(SubscriptionOperation.OpData opData) {
        SubscriptionOperation subOp = opStore.getSubOp(opData.getOperationId());
        subOp.update(opData);
        opStore.updateSubOp(subOp);
    }

    public void updateShardOp(ShardOperation.OpData opData) {
        ShardOperation shardOp = opStore.getShardOp(opData.getOperationId());
        shardOp.update(opData);
        opStore.updateShardOp(shardOp);
    }

    public ShardOperation.StartData requestShardStart(SubscriptionUnitShard shard, VaradhiSubscription subscription) {
        ShardOperation startOp = ShardOperation.startOp(shard, subscription);
        opStore.createShardOp(startOp);
        return (ShardOperation.StartData) startOp.getOpData();
    }
}
