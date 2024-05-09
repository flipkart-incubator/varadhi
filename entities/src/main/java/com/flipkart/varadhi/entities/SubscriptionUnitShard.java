package com.flipkart.varadhi.entities;

import lombok.Getter;

@Getter
public class SubscriptionUnitShard extends SubscriptionShards {

    private final int shardId;

    private final RetryTopic retryTopic;

    private final InternalCompositeTopic deadLetterTopic;

    private final MemberResources requests;

    public SubscriptionUnitShard(int shardId, MemberResources requests, RetryTopic retryTopic, InternalCompositeTopic deadLetterTopic) {
        super(1);
        this.shardId = shardId;
        this.retryTopic = retryTopic;
        this.deadLetterTopic = deadLetterTopic;
        this.requests = requests;
    }

    @Override
    public SubscriptionUnitShard getShard(int shardId) {
        return this.shardId == shardId ? this : null;
    }
}
