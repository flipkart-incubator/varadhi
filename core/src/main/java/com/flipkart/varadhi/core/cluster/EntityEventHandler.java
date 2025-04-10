package com.flipkart.varadhi.core.cluster;

import com.flipkart.varadhi.common.events.EntityEvent;
import com.flipkart.varadhi.entities.auth.ResourceType;

import java.util.Set;

public interface EntityEventHandler {
    void handleEvent(EntityEvent event);

    Set<ResourceType> getSupportedResourceTypes();
}
