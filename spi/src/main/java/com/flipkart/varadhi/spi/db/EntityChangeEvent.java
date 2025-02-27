package com.flipkart.varadhi.spi.db;

import com.flipkart.varadhi.entities.auth.ResourceType;

public interface EntityChangeEvent {

    String getResourceName();

    ResourceType getResourceType();

    void complete();
}
