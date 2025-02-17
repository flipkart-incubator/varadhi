package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.entities.auth.ResourceType;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Represents an event marker in the system that tracks changes to resources.
 * This class extends {@link MetaStoreEntity} and provides immutable event tracking
 * capabilities with timestamping.
 *
 * <p>Each event marker contains:
 * <ul>
 *     <li>A unique name in the format "event_[sequence_number]" (e.g., "event_1", "event_2")</li>
 *     <li>The resource name that was affected</li>
 *     <li>The type of resource that was modified</li>
 *     <li>A timestamp of when the event occurred</li>
 * </ul>
 *
 * <p>Event markers are stored in ZooKeeper with the following path structure:
 * <pre>
 * /varadhi/entities/Event/[eventName]
 * </pre>
 *
 * <p>The sequence number in the event name is generated using a distributed atomic counter
 * to ensure uniqueness across the cluster. This sequence number can be used to determine
 * the order of events in the system.
 *
 * <p>Event markers are immutable once created and are used to track:
 * <ul>
 *     <li>Resource creation events</li>
 *     <li>Resource update events</li>
 *     <li>Resource deletion events (both soft and hard deletes)</li>
 *     <li>Resource restoration events</li>
 * </ul>
 *
 * @see MetaStoreEntity
 * @see ResourceType
 */
@Getter
@EqualsAndHashCode (callSuper = true)
public class EventMarker extends MetaStoreEntity {

    private final String resourceName;
    private final ResourceType resourceType;
    private final long timestamp;

    /**
     * Private constructor to enforce object creation through factory method.
     * Ensures immutability and proper initialization of all fields.
     *
     * @param name           The unique name of the event
     * @param version        The version of the event marker
     * @param resourceName   The name to the resource this event relates to
     * @param resourceType   The type of resource being tracked
     * @param timestamp      The time when this event was created
     */
    private EventMarker(String name, int version, String resourceName, ResourceType resourceType, long timestamp) {
        super(name, version);
        this.resourceName = resourceName;
        this.resourceType = resourceType;
        this.timestamp = timestamp;
    }

    /**
     * Creates a new EventMarker instance with the specified parameters.
     * The timestamp is automatically set to the current system time.
     *
     * <p>This factory method is the recommended way to create new EventMarker instances.
     * It ensures proper initialization and versioning of the event marker.
     *
     * @param name           The unique name of the event
     * @param resourceName   The name to the resource this event relates to
     * @param resourceType   The type of resource being tracked
     * @return A new EventMarker instance
     * @throws IllegalArgumentException if any of the parameters are invalid
     */
    public static EventMarker of(String name, String resourceName, ResourceType resourceType) {
        return new EventMarker(name, INITIAL_VERSION, resourceName, resourceType, System.currentTimeMillis());
    }
}
