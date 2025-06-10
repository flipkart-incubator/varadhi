package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Represents the lifecycle status of an entity with its state, message, and actor code.
 * This class ensures consistency across different entity types such as topics and subscriptions.
 */
@Getter
@AllArgsConstructor (onConstructor = @__ (@JsonCreator))
public class LifecycleStatus {

    /**
     * The current state of the entity.
     */
    private State state;

    /**
     * The message associated with the current state.
     */
    private String message;

    /**
     * The actor code indicating the reason for the current state.
     */
    private ActionCode actionCode;

    /**
     * Constructs a new LifecycleStatus instance with the specified state and actor code.
     * The message is set to the default message of the state.
     *
     * @param state     the state of the entity
     * @param actionCode the actor code indicating the reason for the state
     */
    public LifecycleStatus(State state, ActionCode actionCode) {
        this(state, state.getDefaultMessage(), actionCode);
    }

    /**
     * Updates the lifecycle status with the specified state, message, and actor code.
     *
     * @param state     the new state of the entity
     * @param message   the new message associated with the state
     * @param actionCode the new actor code indicating the reason for the state
     */
    public void update(State state, String message, ActionCode actionCode) {
        if (this.state == state) {
            throw new IllegalArgumentException("Resource is already in " + state + " state");
        }
        this.state = state;
        this.message = (message != null && !message.isEmpty()) ? message : state.getDefaultMessage();
        this.actionCode = actionCode;
    }

    /**
     * Updates the lifecycle status with the specified state.
     * The message is set to the default message of the state.
     * The actor code remains unchanged.
     *
     * @param state the new state of the entity
     */
    public void update(State state) {
        update(state, state.getDefaultMessage(), this.actionCode);
    }

    /**
     * Updates the lifecycle status with the specified state and message.
     * The actor code remains unchanged.
     *
     * @param state   the new state of the entity
     * @param message the new message associated with the state
     */
    public void update(State state, String message) {
        update(state, message, this.actionCode);
    }

    /**
     * Enum representing the possible states of an entity.
     */
    @Getter
    @RequiredArgsConstructor
    public enum State {
        CREATING("Creation in progress."), CREATED("Successfully created."), CREATE_FAILED("Creation failed."),

        DELETING("Deletion in progress."), DELETE_FAILED("Deletion failed."),

        INACTIVE("Currently inactive.");

        /**
         * The default message associated with the state.
         */
        private final String defaultMessage;

        /**
         * Checks if the state is retriable.
         *
         * @return true if the state is CREATE_FAILED or DELETE_FAILED, false otherwise.
         */
        public boolean isRetriable() {
            return this == CREATE_FAILED || this == DELETE_FAILED;
        }

        /**
         * Checks if the state is terminal.
         *
         * @return true if the state is CREATED or INACTIVE, false otherwise.
         */
        public boolean isTerminal() {
            return this == CREATED || this == INACTIVE;
        }
    }


    /**
     * Enum representing the actor codes for entity actions.
     */
    public enum ActionCode {
        //        USER_INITIATED_ACTION,  // Action initiated directly by the user.
        //        USER_REQUESTED_ADMIN_ACTION,  // Action requested by the user to be performed by an admin.
        //        ADMIN_FORCED_ACTION, // Action intentionally performed by an admin.
        //        SYSTEM_ACTION; // Action performed by the system due to policy.

        USER_ACTION, ADMIN_ACTION, SYSTEM_ACTION;

        /**
         * Checks if the action is allowed to be performed by the user.
         *
         * @return true if the action is USER_ACTION, false otherwise.
         */
        public boolean isUserAllowed() {
            return this == USER_ACTION;
        }
    }
}
