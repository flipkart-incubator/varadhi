package com.flipkart.varadhi.spi.db;


import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;

import java.util.List;

public interface OpStore {
    void createSubOp(SubscriptionOperation operation);

    SubscriptionOperation getSubOp(String operationId);


    void updateSubOp(SubscriptionOperation operation);


    void createShardOp(ShardOperation operation);

    List<ShardOperation> getShardOps(String operationId);

    ShardOperation getShardOp(String operationId);

    void updateShardOp(ShardOperation operation);
}
