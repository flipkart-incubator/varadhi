package com.flipkart.varadhi.entities;

import lombok.AllArgsConstructor;
import lombok.Value;


@AllArgsConstructor
public class SubscriptionShard {

    private final int shardId;

    private final ShardAssignment[] assignments;

    private final RetryTopic rqTopic;

    private final VaradhiTopic dlqTopic;

    @Value
    public static class ShardAssignment {
        int storageTopicIdx;
        int[] assignedPartitions;
    }
}
