package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.common.events.EntityEvent;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.spi.db.MetaStore;

public interface EntityEventFactory {
    EntityEvent<?> createEvent(ResourceType resourceType, String resourceName, MetaStore metaStore);
}
