package com.flipkart.varadhi.common.events;

import com.flipkart.varadhi.entities.auth.ResourceType;

public record EntityEvent<T>(
    ResourceType resourceType,
    String resourceName,
    ResourceOperation operation,
    T resourceState
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

    public static <T> EntityEvent<T> of(
        ResourceType resourceType,
        String resourceName,
        ResourceOperation resourceOperation,
        T resourceState
    ) {
        return new EntityEvent<>(resourceType, resourceName, resourceOperation, resourceState);
    }
}
