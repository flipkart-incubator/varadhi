package com.flipkart.varadhi.db.entities;

import com.flipkart.varadhi.db.ZKMetaStore;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.spi.db.MetaStoreChangeEvent;

import java.util.Objects;

/**
 * Implementation of {@link MetaStoreChangeEvent} for ZooKeeper-based metadata store events.
 * This class represents events that occur when resources are modified in the ZooKeeper metadata store.
 *
 * @see MetaStoreChangeEvent
 * @see ZKMetaStore
 */
public class ZKMetadataEvent implements MetaStoreChangeEvent {
    private final String path;
    private final String resourceName;
    private final ResourceType resourceType;
    private final ZKMetaStore zkMetaStore;
    private volatile boolean completed;

    /**
     * Constructs a new ZkEvent with the specified parameters.
     *
     * @param path         the ZooKeeper path where the event occurred
     * @param resourceName the name of the resource that was modified
     * @param resourceType the type of the resource that was modified
     * @param zkMetaStore  the ZooKeeper metadata store instance
     */
    public ZKMetadataEvent(String path, String resourceName, ResourceType resourceType, ZKMetaStore zkMetaStore) {
        this.path = Objects.requireNonNull(path, "path cannot be null.");
        this.resourceName = Objects.requireNonNull(resourceName, "resourceName cannot be null.");
        this.resourceType = Objects.requireNonNull(resourceType, "resourceType cannot be null.");
        this.zkMetaStore = Objects.requireNonNull(zkMetaStore, "zkMetaStore cannot be null.");
        this.completed = false;
    }

    @Override
    public String getResourceName() {
        return resourceName;
    }

    @Override
    public ResourceType getResourceType() {
        return resourceType;
    }

    /**
     * Marks the event as processed by deleting the event marker in ZooKeeper.
     * This method is idempotent and thread-safe.
     *
     * @throws IllegalStateException if the event marker cannot be deleted
     */
    @Override
    public synchronized void markAsProcessed() {
        if (!completed) {
            zkMetaStore.deleteEventZNode(path);
            completed = true;
        }
    }
}
