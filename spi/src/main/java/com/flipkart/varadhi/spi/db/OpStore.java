package com.flipkart.varadhi.spi.db;

import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;

import java.util.List;

/**
 * Interface for storing and managing subscription and shard operations.
 * This interface provides methods for creating, retrieving, and updating
 * subscription and shard operations, as well as checking the existence of shard operations.
 */
public interface OpStore {

    void createSubOp(SubscriptionOperation operation);

    SubscriptionOperation getSubOp(String operationId);

    List<SubscriptionOperation> getPendingSubOps();

    void updateSubOp(SubscriptionOperation operation);

    void createShardOp(ShardOperation operation);

    ShardOperation getShardOp(String operationId);

    List<ShardOperation> getShardOps(String subOpId);

    boolean shardOpExists(String shardOpId);

    void updateShardOp(ShardOperation operation);
}
