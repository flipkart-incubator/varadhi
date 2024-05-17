package com.flipkart.varadhi.spi.db;


import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;

public interface OpStore {
    void createSubOp(SubscriptionOperation operation);

    void createShardOp(ShardOperation operation);

    SubscriptionOperation getSubOp(String operationId);

    ShardOperation getShardOp(String operationId);

    void updateSubOp(SubscriptionOperation operation);

    void updateShardOp(ShardOperation operation);
}
