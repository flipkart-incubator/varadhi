package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.entities.auth.ResourceType;

/**
 * Internal event representing resource state changes in the Varadhi.
 * Used for propagating resource modifications across the cluster.
 *
 * <p>The event name follows the format "event_[sequence]" to maintain ordering
 * of events in the distributed system.
 *
 * <p>Example usage:
 * <pre>
 * ResourceEvent event = ResourceEvent.of(
 *     "event_123",
 *     ResourceType.TOPIC,
 *     "my-topic",
 *     ResourceOperation.UPSERT,
 *     topicState
 * );
 * </pre>
 */
public record ResourceEvent(
        String eventName,
        ResourceType resourceType,
        String resourceName,
        ResourceOperation operation,
        Object resourceState
) {
    public ResourceEvent {
        if (eventName == null || eventName.isBlank()) {
            throw new IllegalArgumentException("eventName cannot be null or blank");
        }
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
     * Creates a new ResourceEvent instance.
     *
     * @param eventName     The unique event identifier (format: event_[sequence])
     * @param resourceType  The type of resource being modified
     * @param resourceName  The name of the resource being modified
     * @param operation     The operation being performed on the resource
     * @param resourceState The current state of the resource (can be null)
     * @return A new ResourceEvent instance
     * @throws IllegalArgumentException if required parameters are null or invalid
     */
    public static ResourceEvent of(
            String eventName,
            ResourceType resourceType,
            String resourceName,
            ResourceOperation operation,
            Object resourceState
    ) {
        return new ResourceEvent(eventName, resourceType, resourceName, operation, resourceState);
    }
}