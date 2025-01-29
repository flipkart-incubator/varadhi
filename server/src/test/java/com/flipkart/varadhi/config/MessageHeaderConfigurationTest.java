package com.flipkart.varadhi.config;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MessageHeaderConfigurationTest {

    @Test
    void testValidConfiguration() throws IllegalAccessException {
        MessageHeaderConfiguration config = MessageHeaderConfiguration.getDefaultMessageHeaders();

        assertTrue(MessageHeaderConfiguration.validateHeaderMapping(config));
    }

    @Test
    void testInvalidMessageId() throws IllegalAccessException {
        MessageHeaderConfiguration config = MessageHeaderConfiguration.builder()
                .msgHeaderPrefix(Arrays.asList("X_", "X-"))
                .messageId("Y_RESTBUS_MESSAGE_ID")  // Invalid messageId
                .groupId("X_RESTBUS_GROUP_ID")
                .build();

        assertFalse(MessageHeaderConfiguration.validateHeaderMapping(config));
    }

    @Test
    void testValidHeaderPrefix() throws IllegalAccessException {
        MessageHeaderConfiguration config = MessageHeaderConfiguration.builder()
                .msgHeaderPrefix(Arrays.asList("Y_"))
                .messageId("Y_RESTBUS_MESSAGE_ID")  // Invalid messageId
                .groupId("Y_RESTBUS_GROUP_ID")
                .build();

        assertTrue(MessageHeaderConfiguration.validateHeaderMapping(config));
    }
}
