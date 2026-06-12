package com.flipkart.varadhi.entities;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

class MessageSizeProfileTest {

    @Test
    void getDefault_ReturnsSaneValues() {
        MessageSizeProfile profile = MessageSizeProfile.getDefault();

        assertAll(
            () -> assertEquals(1024, profile.getAvgMsgSizeBytes()),
            () -> assertEquals(1024, profile.getMaxMsgSizeBytes())
        );
    }

    @Test
    void jsonRoundTrip_PreservesFields() {
        MessageSizeProfile profile = new MessageSizeProfile(512, 2048);
        MessageSizeProfile deserialized = JsonMapper.jsonDeserialize(
            JsonMapper.jsonSerialize(profile),
            MessageSizeProfile.class
        );

        assertAll(
            () -> assertEquals(profile.getAvgMsgSizeBytes(), deserialized.getAvgMsgSizeBytes()),
            () -> assertEquals(profile.getMaxMsgSizeBytes(), deserialized.getMaxMsgSizeBytes())
        );
    }
}
