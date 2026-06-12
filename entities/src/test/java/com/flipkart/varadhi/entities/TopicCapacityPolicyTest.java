package com.flipkart.varadhi.entities;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TopicCapacityPolicyTest {

    @Test
    void applyFloors_FloorsQpsAndThroughputBelowOne() {
        TopicCapacityPolicy policy = new TopicCapacityPolicy(0, 0, 2, 7);
        TopicCapacityPolicy floored = policy.applyFloors();

        assertAll(
            () -> assertEquals(1, floored.getQps()),
            () -> assertEquals(1, floored.getThroughputKBps()),
            () -> assertEquals(2, floored.getReadFanOut()),
            () -> assertEquals(7, floored.getRetentionPeriodInDays())
        );
    }

    @Test
    void applyFloors_LeavesValuesAtOrAboveOneUnchanged() {
        TopicCapacityPolicy policy = new TopicCapacityPolicy(100, 400, 2, 7);
        TopicCapacityPolicy floored = policy.applyFloors();

        assertAll(() -> assertEquals(100, floored.getQps()), () -> assertEquals(400, floored.getThroughputKBps()));
    }
}
