package com.flipkart.varadhi.common.events;

import com.flipkart.varadhi.entities.Resource;

/**
 * Listener interface for entity change events in Varadhi.
 * <p>
 * This interface defines a callback mechanism for components that need to be notified
 * when resource change. Implementations of this interface can register
 * with event sources to receive notifications about entity changes.
 * <p>
 * This is a functional interface whose functional method is {@link #onChange(ResourceEvent)}.
 *
 * @see ResourceEvent
 */
@FunctionalInterface
public interface ResourceEventListener<T extends Resource> {

    /**
     * Called when an entity changes.
     * <p>
     * This method is invoked by event sources when an entity is created, updated, or deleted.
     * Implementations should process the event appropriately based on their specific requirements.
     *
     * @param event the event containing information about the entity change
     * @throws IllegalStateException if the listener is in a state where it cannot process events
     */
    void onChange(ResourceEvent<? extends T> event);
}
