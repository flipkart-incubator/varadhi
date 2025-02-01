package com.flipkart.varadhi.entities;

import jakarta.validation.constraints.NotNull;

/**
 * Represents a request for an action on a resource, containing an action code and an optional message.
 *
 * @param actionCode the action code indicating the action to be performed, must not be null
 * @param message    an optional message associated with the action
 */
public record ResourceActionRequest(@NotNull LifecycleStatus.ActionCode actionCode, String message)
        implements Validatable {

    public ResourceActionRequest {
        if (actionCode == null) {
            throw new NullPointerException("actionCode must not be null");
        }
    }
}
