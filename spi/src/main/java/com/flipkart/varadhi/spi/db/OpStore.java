package com.flipkart.varadhi.spi.db;

import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.entities.cluster.failover.TopicFailoverOperation;

import java.util.List;

/**
 * Interface for storing and managing subscription, shard, and topic-failover operations.
 *
 * <p>Topic-failover ops are stored as untracked znodes (no L1 fan-out): controllers
 * discover live ops by listing this collection on leader election, and pods never
 * read from it directly.
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

    /* ============== Topic Failover Operations ============== */

    /** Creates a new topic-failover op. Untracked: no L1 event is emitted. */
    void createTopicFailoverOp(TopicFailoverOperation operation);

    /** Loads a single topic-failover op by id, refreshing its version from ZK. */
    TopicFailoverOperation getTopicFailoverOp(String operationId);

    /** Updates an existing topic-failover op (untracked, version-checked). */
    void updateTopicFailoverOp(TopicFailoverOperation operation);

    /** Deletes a topic-failover op. */
    void deleteTopicFailoverOp(String operationId);

    /** Returns {@code true} if a topic-failover op with this id is persisted. */
    boolean topicFailoverOpExists(String operationId);

    /** Lists every persisted topic-failover op (terminal and non-terminal). */
    List<TopicFailoverOperation> getAllTopicFailoverOps();

    /** Lists only ops whose state is not yet terminal (used on leader-election rehydrate). */
    List<TopicFailoverOperation> getActiveTopicFailoverOps();
}
