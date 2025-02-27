package com.flipkart.varadhi.db.entities;

import com.flipkart.varadhi.db.ZKMetaStore;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.spi.db.IEventMarker;
import lombok.Getter;

public class EventMarker implements IEventMarker {
    private final String path;
    @Getter
    private final String resourceName;
    @Getter
    private final ResourceType resourceType;
    private final ZKMetaStore zkMetaStore;
    private boolean completed = false;

    public EventMarker(String path, String resourceName, ResourceType resourceType, ZKMetaStore zkMetaStore) {
        this.path = path;
        this.resourceName = resourceName;
        this.resourceType = resourceType;
        this.zkMetaStore = zkMetaStore;
    }

    @Override
    public void complete() {
        if (!completed) {
            zkMetaStore.deleteEventMarker(path);
            completed = true;
        }
    }
}
