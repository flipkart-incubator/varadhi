package com.flipkart.varadhi.common.events;

import com.flipkart.varadhi.entities.auth.ResourceType;

/**
 * Represents an immutable event related to an entity in Varadhi.
 * <p>
 * This record encapsulates information about changes to entities in Varadhi,
 * providing details about what resource was modified, how it was modified, and the
 * actual resource data. It also includes a mechanism to mark the event as processed.
 * <p>
 * The generic type parameter {@code T} allows this event to carry any type of resource.
 *
 * @param <T> the type of resource this event carries
 */
public record EntityEvent<T>(
    ResourceType resourceType,
    String resourceName,
    EventType operation,
    T resource,
    Runnable commiter
) {
    /**
     * Constructs a new EntityEvent with validation of required fields.
     *
     * @throws IllegalArgumentException if resourceType is null, resourceName is null or blank,
     *                                  or operation is null
     */
    public EntityEvent {
        if (resourceType == null) {
            throw new IllegalArgumentException("resourceType cannot be null");
        }
        if (resourceName == null || resourceName.isBlank()) {
            throw new IllegalArgumentException("resourceName cannot be null or blank");
        }
        if (operation == null) {
            throw new IllegalArgumentException("operation cannot be null");
        }
    }

    /**
     * Marks this event as processed by executing the committer callback if present.
     * This method should be called after the event has been successfully processed
     * by all relevant components in the system.
     */
    public void markAsProcessed() {
        if (commiter != null) {
            commiter.run();
        }
    }
}
