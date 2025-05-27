package com.flipkart.varadhi.common.events;

/**
 * Represents the types of operations that can be performed on resource in Varadhi.
 * <p>
 * This enum defines the standard event types that can be associated with entity changes,
 * allowing components to determine how to process these events based on their type.
 *
 * @see ResourceEvent
 */
public enum EventType {

    /**
     * Indicates that an entity should be invalidated or removed.
     * <p>
     * This event type is typically used when an entity has been deleted or
     * significantly changed, requiring any instances to be discarded.
     */
    INVALIDATE,

    /**
     * Indicates that an entity has been created or updated.
     * <p>
     * This event type is used when an entity is newly created or when an existing
     * entity has been modified, requiring to update their instances.
     */
    UPSERT
}
