package com.flipkart.varadhi.core.topic;

import com.flipkart.varadhi.entities.MessageSizeProfile;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TopicCapacityConsistencyValidatorTest {

    @Test
    void validate_RejectsWhenThroughputBelowMaxSizedDemand() {
        TopicCapacityPolicy capacity = new TopicCapacityPolicy(10, 5, 2, 2);
        MessageSizeProfile profile = new MessageSizeProfile(1024, 2048);

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> TopicCapacityConsistencyValidator.validate(capacity, profile)
        );
        assertTrue(ex.getMessage().contains("maxMsgSizeBytes"));
    }

    @Test
    void validate_AllowsWhenThroughputMeetsMaxSizedDemand() {
        TopicCapacityPolicy capacity = new TopicCapacityPolicy(10, 25, 2, 2);
        MessageSizeProfile profile = new MessageSizeProfile(1024, 2048);

        assertDoesNotThrow(() -> TopicCapacityConsistencyValidator.validate(capacity, profile));
    }
}
