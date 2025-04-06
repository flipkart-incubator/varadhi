package com.flipkart.varadhi.common.events;

import com.flipkart.varadhi.entities.auth.ResourceType;

public record EntityEvent<T>(
    ResourceType resourceType,
    String resourceName,
    EventType operation,
    T resource,
    Runnable commiter
) {
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

    public void markAsProcessed() {
        commiter.run();
    }
}
