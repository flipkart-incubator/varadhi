package com.flipkart.varadhi.core;

import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.Validatable;
import jakarta.validation.constraints.NotNull;

import java.util.Objects;

/**
 * Represents the type of action on a resource, containing an action  code and an optional message.
 *
 * @param actionCode the action code indicating the source of action to be performed, must not be null
 * @param message   an optional message associated with the action
 */
public record RequestActionType(@NotNull LifecycleStatus.ActionCode actionCode, String message) implements Validatable {

    public RequestActionType {
        Objects.requireNonNull(actionCode, "actorCode must not be null");
    }
}
