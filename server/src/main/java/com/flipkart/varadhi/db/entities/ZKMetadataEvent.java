package com.flipkart.varadhi.db.entities;

import com.flipkart.varadhi.db.ZKMetaStore;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.spi.db.MetaStoreChangeEvent;

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
     * @throws IllegalArgumentException if any of the parameters are null or invalid
     */
    public ZKMetadataEvent(String path, String resourceName, ResourceType resourceType, ZKMetaStore zkMetaStore) {
        this.path = validateNotNull(path, "path");
        this.resourceName = validateNotNull(resourceName, "resourceName");
        this.resourceType = validateNotNull(resourceType, "resourceType");
        this.zkMetaStore = validateNotNull(zkMetaStore, "zkMetaStore");
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

    /**
     * Validates that the input parameter is not null.
     *
     * @param param     the parameter to validate
     * @param paramName the name of the parameter for error messaging
     * @param <T>       the type of the parameter
     * @return the validated parameter
     * @throws IllegalArgumentException if the parameter is null
     */
    private static <T> T validateNotNull(T param, String paramName) {
        if (param == null) {
            throw new IllegalArgumentException(paramName + " cannot be null");
        }
        return param;
    }
}
