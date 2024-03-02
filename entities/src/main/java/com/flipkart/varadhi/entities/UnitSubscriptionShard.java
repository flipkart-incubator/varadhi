package com.flipkart.varadhi.entities;

import lombok.Getter;

@Getter
public class UnitSubscriptionShard extends SubscriptionShards {

    private final int shardId;

    private final RetryTopic rqTopic;

    private final InternalCompositeTopic dlqTopic;

    public UnitSubscriptionShard(int shardId, RetryTopic rqTopic, InternalCompositeTopic dlqTopic) {
        super(1);
        this.shardId = shardId;
        this.rqTopic = rqTopic;
        this.dlqTopic = dlqTopic;
    }
}
