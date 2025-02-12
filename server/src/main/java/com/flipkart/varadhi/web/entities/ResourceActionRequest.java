package com.flipkart.varadhi.web.entities;

import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.Validatable;
import jakarta.validation.constraints.NotNull;

import java.util.Objects;

/**
 * Represents a request for an action on a resource, containing an action code and an optional message.
 *
 * @param actorCode the action code indicating the action to be performed, must not be null
 * @param message   an optional message associated with the action
 */
public record ResourceActionRequest(@NotNull LifecycleStatus.ActorCode actorCode, String message) implements Validatable {

    public ResourceActionRequest {
        Objects.requireNonNull(actorCode, "actorCode must not be null");
    }
}
