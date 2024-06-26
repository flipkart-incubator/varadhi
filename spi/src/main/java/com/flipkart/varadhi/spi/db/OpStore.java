package com.flipkart.varadhi.spi.db;


import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;

import java.util.List;

public interface OpStore {
    void createSubOp(SubscriptionOperation operation);

    SubscriptionOperation getSubOp(String operationId);

    List<SubscriptionOperation> getPendingSubOps();

    void updateSubOp(SubscriptionOperation operation);

    void createShardOp(ShardOperation operation);

    boolean shardOpExists(String shardOpId);

    List<ShardOperation> getShardOps(String subOpId);

    ShardOperation getShardOp(String operationId);

    void updateShardOp(ShardOperation operation);
}
