package com.flipkart.varadhi.core.cluster.events;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.flipkart.varadhi.entities.Resource;
import com.flipkart.varadhi.entities.ResourceType;

/**
 * Represents an immutable event related to an entity in Varadhi.
 * <p>
 * This record encapsulates information about changes to resource in Varadhi,
 * providing details about what resource was modified, how it was modified, and the
 * actual resource data. It also includes a mechanism to mark the event as processed.
 * <p>
 * The generic type parameter {@code T} allows this event to carry any type of resource.
 *
 * @param <T> the type of resource this event carries
 */
public record ResourceEvent<T extends Resource>(
    ResourceType resourceType,
    String resourceName,
    EventType operation,
    T resource,
    int version,
    @JsonIgnore Runnable commiter
) {
    /**
     * Constructs a new ResourceEvent with validation of required fields.
     *
     * @throws IllegalArgumentException if resourceType is null, resourceName is null or blank,
     *                                  or operation is null
     */
    public ResourceEvent {
        if (resourceType == null) {
            throw new IllegalArgumentException("resourceType cannot be null");
        }
        if (resourceName == null || resourceName.isBlank()) {
            throw new IllegalArgumentException("resourceName cannot be null or blank");
        }
        if (operation == null) {
            throw new IllegalArgumentException("operation cannot be null");
        }
        if (version < 0) {
            throw new IllegalArgumentException("version cannot be negative");
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
