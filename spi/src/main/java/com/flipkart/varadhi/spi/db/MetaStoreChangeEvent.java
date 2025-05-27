package com.flipkart.varadhi.spi.db;

import com.flipkart.varadhi.entities.auth.EntityType;

/**
 * Represents a change event in the metadata store.
 * This interface defines the contract for events that occur when metadata
 * resources are modified in the system.
 *
 */
public interface MetaStoreChangeEvent {

    /**
     * Retrieves the name of the resource that was modified.
     *
     * @return the name of the modified resource
     * @throws IllegalStateException if the resource name is not available
     */
    String getResourceName();

    /**
     * Retrieves the type of the resource that was modified.
     *
     * @return the {@link EntityType} of the modified resource
     * @throws IllegalStateException if the resource type is not available
     */
    EntityType getEntityType();

    /**
     * Marks the event as processed, indicating that all necessary actions
     * have been completed for this event.
     *
     * @throws IllegalStateException if the event cannot be marked as processed in its current state
     */
    void markAsProcessed();
}
