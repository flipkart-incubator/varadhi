package com.flipkart.varadhi.core.cluster;

import com.flipkart.varadhi.common.events.EntityEvent;

/**
 * Listener interface for entity events in the Varadhi system.
 * <p>
 * Implementations of this interface can process entity events for different resource types
 * such as projects, topics, etc. The listener is responsible for handling state changes
 * based on the events received.
 */
public interface EventListener {

    /**
     * Processes an entity event.
     * <p>
     * This method is called when an entity event is received from any source (local or remote).
     * Implementations should handle the event based on its type and resource type.
     *
     * @param event the entity event to process
     * @throws IllegalArgumentException if the event is invalid or cannot be processed
     */
    void processEvent(EntityEvent<?> event);
}
