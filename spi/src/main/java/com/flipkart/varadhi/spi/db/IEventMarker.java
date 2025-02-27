package com.flipkart.varadhi.spi.db;

import com.flipkart.varadhi.entities.auth.ResourceType;

public interface IEventMarker {

    String getResourceName();

    ResourceType getResourceType();

    void complete();
}
