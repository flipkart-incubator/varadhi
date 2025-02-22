package com.flipkart.varadhi.entities;

/**
 * Represents operations that can be performed on resources in the Varadhi.
 * Used by {@link ResourceEvent} to indicate the type of state change.
 *
 * <p>Supported operations:
 * <ul>
 *   <li>{@link #INVALIDATE} - Marks a resource as invalid/deleted</li>
 *   <li>{@link #UPSERT} - Creates or updates a resource</li>
 * </ul>
 *
 * <p>This enum is used in conjunction with {@link EventMarker} and {@link ResourceEvent}
 * to track resource state changes across the distributed system.
 */
public enum ResourceOperation {
    INVALIDATE,
    UPSERT
}
