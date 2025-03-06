package com.flipkart.varadhi.spi.db;

/**
 * Functional interface for listening to metadata store change events.
 * Implementations of this interface can handle various metadata store changes
 * in the system.
 *
 * @see MetaStoreChangeEvent
 */
@FunctionalInterface
public interface MetaStoreEventListener {

    /**
     * Called when a change event occurs in the metadata store.
     *
     * @param event the metadata store change event containing relevant information
     * @throws IllegalStateException if the event cannot be processed
     */
    void onEvent(MetaStoreChangeEvent event);
}
