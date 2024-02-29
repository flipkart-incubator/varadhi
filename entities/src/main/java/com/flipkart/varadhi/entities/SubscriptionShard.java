package com.flipkart.varadhi.entities;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Value;

@Getter
@AllArgsConstructor
public class SubscriptionShard {

    private final int shardId;

    private final ShardAssignment[] assignments;

    private final RetryTopic rqTopic;

    private final InternalCompositeTopic dlqTopic;

    @Value
    public static class ShardAssignment {
        int storageTopicIdx;
        int[] assignedPartitions;
    }
}
