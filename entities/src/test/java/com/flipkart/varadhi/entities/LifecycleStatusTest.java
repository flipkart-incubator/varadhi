package com.flipkart.varadhi.entities;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LifecycleStatusTest {

    @Test
    void constructor_SetsStateAndActionCode() {
        LifecycleStatus status =
                new LifecycleStatus(LifecycleStatus.State.CREATED, LifecycleStatus.ActionCode.SYSTEM_ACTION);
        assertAll(
                () -> assertEquals(LifecycleStatus.State.CREATED, status.getState()),
                () -> assertEquals("Successfully created.", status.getMessage()),
                () -> assertEquals(LifecycleStatus.ActionCode.SYSTEM_ACTION, status.getActionCode())
        );
    }

    @Test
    void update_ChangesStateMessageAndActionCode() {
        LifecycleStatus status =
                new LifecycleStatus(LifecycleStatus.State.CREATING, LifecycleStatus.ActionCode.SYSTEM_ACTION);
        status.update(
                LifecycleStatus.State.CREATED, "Created successfully",
                LifecycleStatus.ActionCode.USER_ACTION
        );
        assertAll(
                () -> assertEquals(LifecycleStatus.State.CREATED, status.getState()),
                () -> assertEquals("Created successfully", status.getMessage()),
                () -> assertEquals(LifecycleStatus.ActionCode.USER_ACTION, status.getActionCode())
        );
    }

    @Test
    void update_ChangesStateAndMessageOnly() {
        LifecycleStatus status =
                new LifecycleStatus(LifecycleStatus.State.CREATING, LifecycleStatus.ActionCode.SYSTEM_ACTION);
        status.update(LifecycleStatus.State.CREATED, "Created successfully");
        assertAll(
                () -> assertEquals(LifecycleStatus.State.CREATED, status.getState()),
                () -> assertEquals("Created successfully", status.getMessage()),
                () -> assertEquals(LifecycleStatus.ActionCode.SYSTEM_ACTION, status.getActionCode())
        );
    }

    @Test
    void update_NullMessageUsesDefaultMessage() {
        LifecycleStatus status =
                new LifecycleStatus(LifecycleStatus.State.CREATING, LifecycleStatus.ActionCode.SYSTEM_ACTION);
        status.update(LifecycleStatus.State.CREATED, null);
        assertAll(
                () -> assertEquals(LifecycleStatus.State.CREATED, status.getState()),
                () -> assertEquals("Successfully created.", status.getMessage()),
                () -> assertEquals(LifecycleStatus.ActionCode.SYSTEM_ACTION, status.getActionCode())
        );
    }

    @Test
    void actionCode_isUserAllowed_ReturnsCorrectValue() {
        assertTrue(LifecycleStatus.ActionCode.USER_ACTION.isUserAllowed());
        assertFalse(LifecycleStatus.ActionCode.ADMIN_ACTION.isUserAllowed());
        assertFalse(LifecycleStatus.ActionCode.SYSTEM_ACTION.isUserAllowed());
    }
}
