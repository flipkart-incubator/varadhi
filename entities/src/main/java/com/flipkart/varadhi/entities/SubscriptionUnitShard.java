package com.flipkart.varadhi.entities;

import lombok.Getter;

@Getter
public class SubscriptionUnitShard extends SubscriptionShards {

    private final int shardId;
    private final RetryTopic retryTopic;
    private final InternalCompositeTopic deadLetterTopic;
    private final CapacityPolicy capacityRequest;

    public SubscriptionUnitShard(
            int shardId, CapacityPolicy capacityRequest, RetryTopic retryTopic, InternalCompositeTopic deadLetterTopic
    ) {
        super(1);
        this.shardId = shardId;
        this.retryTopic = retryTopic;
        this.deadLetterTopic = deadLetterTopic;
        this.capacityRequest = capacityRequest;
    }

    @Override
    public SubscriptionUnitShard getShard(int shardId) {
        if (shardId == this.shardId) {
            return this;
        }
        throw new IllegalArgumentException("Invalid shard Id, no shard found.");
    }
}
