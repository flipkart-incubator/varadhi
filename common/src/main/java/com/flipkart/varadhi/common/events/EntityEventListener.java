package com.flipkart.varadhi.common.events;

/**
 * Listener interface for entity change events in Varadhi.
 * <p>
 * This interface defines a callback mechanism for components that need to be notified
 * when entities change. Implementations of this interface can register
 * with event sources to receive notifications about entity changes.
 * <p>
 * This is a functional interface whose functional method is {@link #onChange(EntityEvent)}.
 *
 * @see EntityEvent
 */
@FunctionalInterface
public interface EntityEventListener {

    /**
     * Called when an entity changes.
     * <p>
     * This method is invoked by event sources when an entity is created, updated, or deleted.
     * Implementations should process the event appropriately based on their specific requirements.
     *
     * @param event the event containing information about the entity change
     * @throws IllegalStateException if the listener is in a state where it cannot process events
     */
    void onChange(EntityEvent<?> event);
}
