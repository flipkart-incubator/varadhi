package com.flipkart.varadhi.entities;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LifecycleStatusTest {

    @Test
    void constructor_SetsStateAndActionCode() {
        LifecycleStatus status =
                new LifecycleStatus(LifecycleStatus.State.CREATED, LifecycleStatus.ActorCode.SYSTEM_ACTION);
        assertAll(
                () -> assertEquals(LifecycleStatus.State.CREATED, status.getState()),
                () -> assertEquals("Successfully created.", status.getMessage()),
                () -> assertEquals(LifecycleStatus.ActorCode.SYSTEM_ACTION, status.getActorCode())
        );
    }

    @Test
    void update_ChangesStateMessageAndActionCode() {
        LifecycleStatus status =
                new LifecycleStatus(LifecycleStatus.State.CREATING, LifecycleStatus.ActorCode.SYSTEM_ACTION);
        status.update(
                LifecycleStatus.State.CREATED, "Created successfully",
                LifecycleStatus.ActorCode.USER_ACTION
        );
        assertAll(
                () -> assertEquals(LifecycleStatus.State.CREATED, status.getState()),
                () -> assertEquals("Created successfully", status.getMessage()),
                () -> assertEquals(LifecycleStatus.ActorCode.USER_ACTION, status.getActorCode())
        );
    }

    @Test
    void update_ChangesStateAndMessageOnly() {
        LifecycleStatus status =
                new LifecycleStatus(LifecycleStatus.State.CREATING, LifecycleStatus.ActorCode.SYSTEM_ACTION);
        status.update(LifecycleStatus.State.CREATED, "Created successfully");
        assertAll(
                () -> assertEquals(LifecycleStatus.State.CREATED, status.getState()),
                () -> assertEquals("Created successfully", status.getMessage()),
                () -> assertEquals(LifecycleStatus.ActorCode.SYSTEM_ACTION, status.getActorCode())
        );
    }

    @Test
    void update_NullMessageUsesDefaultMessage() {
        LifecycleStatus status =
                new LifecycleStatus(LifecycleStatus.State.CREATING, LifecycleStatus.ActorCode.SYSTEM_ACTION);
        status.update(LifecycleStatus.State.CREATED, null);
        assertAll(
                () -> assertEquals(LifecycleStatus.State.CREATED, status.getState()),
                () -> assertEquals("Successfully created.", status.getMessage()),
                () -> assertEquals(LifecycleStatus.ActorCode.SYSTEM_ACTION, status.getActorCode())
        );
    }

    @Test
    void update_SameState_ThrowsIllegalArgumentException() {
        LifecycleStatus status =
                new LifecycleStatus(LifecycleStatus.State.CREATED, LifecycleStatus.ActorCode.SYSTEM_ACTION);
        assertThrows(
                IllegalArgumentException.class, () ->
                        status.update(
                                LifecycleStatus.State.CREATED, "Already created",
                                LifecycleStatus.ActorCode.USER_ACTION
                        )
        );
    }

    @Test
    void actionCode_isUserAllowed_ReturnsCorrectValue() {
        assertTrue(LifecycleStatus.ActorCode.USER_ACTION.isUserAllowed());
        assertFalse(LifecycleStatus.ActorCode.ADMIN_ACTION.isUserAllowed());
        assertFalse(LifecycleStatus.ActorCode.SYSTEM_ACTION.isUserAllowed());
    }

    @Test
    void isRetriable_ReturnsCorrectValue() {
        assertTrue(LifecycleStatus.State.CREATE_FAILED.isRetriable());
        assertTrue(LifecycleStatus.State.DELETE_FAILED.isRetriable());
        assertFalse(LifecycleStatus.State.CREATED.isRetriable());
        assertFalse(LifecycleStatus.State.CREATING.isRetriable());
    }

    @Test
    void isTerminal_ReturnsCorrectValue() {
        assertTrue(LifecycleStatus.State.CREATED.isTerminal());
        assertTrue(LifecycleStatus.State.INACTIVE.isTerminal());
        assertFalse(LifecycleStatus.State.CREATE_FAILED.isTerminal());
        assertFalse(LifecycleStatus.State.CREATING.isTerminal());
    }
}
