package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.entities.auth.ResourceType;

public record EntityEvent(ResourceType resourceType, String resourceName,
                          CacheOperation operation, Object resourceState) {
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

    public static EntityEvent of(
        ResourceType resourceType,
        String resourceName,
        CacheOperation cacheOperation,
        Object resourceState
    ) {
        return new EntityEvent(resourceType, resourceName, cacheOperation, resourceState);
    }
}
