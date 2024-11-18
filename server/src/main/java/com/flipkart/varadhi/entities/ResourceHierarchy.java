package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.entities.auth.ResourceType;

import java.util.Map;

public interface ResourceHierarchy {
    String getResourcePath();

    Map<String, String> getAttributes();
}
