package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.entities.auth.ResourceType;

import java.util.Map;

public interface ResourceHierarchy {
    String getResourcePath();
    default String getResourcePath(ResourceType type) {
        throw new IllegalArgumentException("Invalid Resource type %s for Root path.".formatted(type));
    }

    Map<String, String> getAttributes();
}
