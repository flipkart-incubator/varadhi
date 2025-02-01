package com.flipkart.varadhi.entities;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ResourceActionRequestTest {

    @Test
    void constructor_SetsActionCodeAndMessage() {
        ResourceActionRequest request = new ResourceActionRequest(
                LifecycleStatus.ActionCode.SYSTEM_ACTION,
                "Test message"
        );
        assertAll(
                () -> assertEquals(LifecycleStatus.ActionCode.SYSTEM_ACTION, request.actionCode()),
                () -> assertEquals("Test message", request.message())
        );
    }

    @Test
    void constructor_NullMessage_SetsActionCodeOnly() {
        ResourceActionRequest request = new ResourceActionRequest(LifecycleStatus.ActionCode.SYSTEM_ACTION, null);
        assertAll(
                () -> assertEquals(LifecycleStatus.ActionCode.SYSTEM_ACTION, request.actionCode()),
                () -> assertNull(request.message())
        );
    }

    @Test
    void constructor_NullActionCode_ThrowsException() {
        assertThrows(
                NullPointerException.class, () -> {
                    new ResourceActionRequest(null, "Test message");
                }
        );
    }

    @Test
    void actionCode_ReturnsCorrectActionCode() {
        ResourceActionRequest request = new ResourceActionRequest(
                LifecycleStatus.ActionCode.USER_ACTION,
                "Test message"
        );
        assertEquals(LifecycleStatus.ActionCode.USER_ACTION, request.actionCode());
    }

    @Test
    void message_ReturnsCorrectMessage() {
        ResourceActionRequest request = new ResourceActionRequest(
                LifecycleStatus.ActionCode.USER_ACTION,
                "Test message"
        );
        assertEquals("Test message", request.message());
    }
}
