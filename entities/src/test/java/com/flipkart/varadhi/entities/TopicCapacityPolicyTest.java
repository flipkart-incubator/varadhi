package com.flipkart.varadhi.entities;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TopicCapacityPolicyTest {

    @Test
    void isConsistentWith_RejectsWhenThroughputBelowMaxSizedDemand() {
        TopicCapacityPolicy capacity = new TopicCapacityPolicy(10, 5, 2, 2);
        MessageSizeProfile profile = new MessageSizeProfile(1024, 2048);

        assertFalse(capacity.isConsistentWith(profile));
    }

    @Test
    void isConsistentWith_AllowsWhenThroughputMeetsMaxSizedDemand() {
        TopicCapacityPolicy capacity = new TopicCapacityPolicy(10, 25, 2, 2);
        MessageSizeProfile profile = new MessageSizeProfile(1024, 2048);

        assertTrue(capacity.isConsistentWith(profile));
    }
}
