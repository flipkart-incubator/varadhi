package com.flipkart.varadhi.entities;

import lombok.Getter;

import java.util.Comparator;

import static java.util.Comparator.comparing;

@Getter
public class SubscriptionUnitShard extends SubscriptionShards {
    //TODO::Add a notion of regions to either Shard or VaradhiSubscription
    // and bring operation down to regional level e.g. start/stop/assignment.
    public static final Comparator<SubscriptionUnitShard> ShardCapacityComparator = comparing(o -> o.capacityRequest);

    private final int shardId;

    // TODO: why is topicCapacity object being used here?
    private final TopicCapacityPolicy capacityRequest;
    private final InternalCompositeSubscription mainSubscription;
    private final RetrySubscription retrySubscription;
    private final InternalCompositeSubscription deadLetterSubscription;

    public SubscriptionUnitShard(
        int shardId,
        TopicCapacityPolicy capacityRequest,
        InternalCompositeSubscription mainSubscription,
        RetrySubscription retrySubscription,
        InternalCompositeSubscription deadLetterSubscription
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
