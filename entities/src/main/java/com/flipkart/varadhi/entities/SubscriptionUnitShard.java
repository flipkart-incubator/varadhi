package com.flipkart.varadhi.entities;

import lombok.Getter;

@Getter
public class SubscriptionUnitShard extends SubscriptionShards {
    //TODO::Add a notion of regions to either Shard or VaradhiSubscription
    // and bring operation down to regional level e.g. start/stop/assignment.
    private final int shardId;
    private final TopicCapacityPolicy capacityRequest;
    private final InternalCompositeSubscription mainSubscription;
    private final RetrySubscription retrySubscription;
    private final InternalCompositeSubscription deadLetterSubscription;

    public SubscriptionUnitShard(
            int shardId, TopicCapacityPolicy capacityRequest, InternalCompositeSubscription mainSubscription,
            RetrySubscription retrySubscription, InternalCompositeSubscription deadLetterSubscription
    ) {
        super(1);
        this.shardId = shardId;
        this.retrySubscription = retrySubscription;
        this.deadLetterSubscription = deadLetterSubscription;
        this.capacityRequest = capacityRequest;
        this.mainSubscription = mainSubscription;
    }

    @Override
    public SubscriptionUnitShard getShard(int shardId) {
        if (shardId == this.shardId) {
            return this;
        }
        throw new IllegalArgumentException("Invalid shard Id, no shard found.");
    }
}
