package com.flipkart.varadhi.spi.db;

import com.flipkart.varadhi.entities.EventMarker;

import java.util.List;

/**
 * Interface defining operations for managing events in a distributed store.
 * Implementations of this interface should provide thread-safe and distributed
 * access to event data.
 */
public interface EventStore {

    void createEvent(EventMarker event);

    EventMarker getEvent(String eventName);

    List<EventMarker> getPendingEvents();

    void deleteEvent(String eventName);

    /**
     * Gets the cache instance used for event notifications.
     * The cache must be started and managed by the consumer.
     *
     * @return The cache instance for events as an Object that should be cast to CuratorCache
     */
    Object getEventCache();

    long getNextSequenceNumber();
}
