package com.flipkart.varadhi.spi.db;

/**
 * Provider interface for metadata storage implementations.
 * <p>
 * This interface defines the contract for metadata store providers that manage
 * various types of storage operations in the system. Implementations must ensure
 * thread-safety and proper resource management.
 * <p>
 * The provider lifecycle follows this sequence:
 * <ol>
 *     <li>Initialize with configuration via {@link #init}</li>
 *     <li>Get specific store implementations via getter methods</li>
 *     <li>Clean up resources when no longer needed</li>
 * </ol>
 *
 * @see MetaStore
 * @see OpStore
 * @see AssignmentStore
 * @see EventStore
 */
public interface MetaStoreProvider extends AutoCloseable {

    /**
     * Initializes the provider with the given configuration.
     * Must be called before using any store methods.
     *
     * @param metaStoreOptions Configuration options for the store
     * @throws IllegalStateException    if provider is already initialized
     * @throws IllegalArgumentException if configuration is invalid
     */
    void init(MetaStoreOptions metaStoreOptions);

    /**
     * Returns the metadata store implementation.
     *
     * @return The metadata store instance
     * @throws IllegalStateException if provider is not initialized
     */
    MetaStore getMetaStore();

    /**
     * Returns the operations store implementation.
     *
     * @return The operations store instance
     * @throws IllegalStateException if provider is not initialized
     */
    OpStore getOpStore();

    /**
     * Returns the assignment store implementation.
     *
     * @return The assignment store instance
     * @throws IllegalStateException if provider is not initialized
     */
    AssignmentStore getAssignmentStore();

    /**
     * Returns the event store implementation.
     *
     * @return The event store instance
     * @throws IllegalStateException if provider is not initialized
     */
    EventStore getEventStore();
}
