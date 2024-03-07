package com.flipkart.varadhi.entities;

import lombok.Getter;

@Getter
public class UnitSubscriptionShard extends SubscriptionShards {

    private final int shardId;

    private final RetryTopic retryTopic;

    private final InternalCompositeTopic deadLetterTopic;

    public UnitSubscriptionShard(int shardId, RetryTopic retryTopic, InternalCompositeTopic deadLetterTopic) {
        super(1);
        this.shardId = shardId;
        this.retryTopic = retryTopic;
        this.deadLetterTopic = deadLetterTopic;
    }
}
