package com.flipkart.varadhi.events;

import com.flipkart.varadhi.core.cluster.entities.MemberInfo;
import lombok.Getter;

/**
 * Represents the state of an event associated with a cluster node.
 * This class is thread-safe and immutable where possible.
 */
@Getter
public class ClusterMemberEventState {
    private final MemberInfo member;
    private final int maxRetries;
    private volatile int retryCount;
    private volatile boolean complete;

    /**
     * Creates a new NodeEventState instance.
     *
     * @param member     The member information associated with this event state
     * @param maxRetries The maximum number of retries allowed
     * @throws IllegalArgumentException if maxRetries is negative
     */
    public ClusterMemberEventState(MemberInfo member, int maxRetries) {
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must be non-negative");
        }
        this.member = member;
        this.maxRetries = maxRetries;
        this.retryCount = 0;
        this.complete = false;
    }

    /**
     * Increments the retry count atomically.
     */
    public synchronized void incrementRetries() {
        retryCount++;
    }

    /**
     * Checks if there are retries remaining.
     *
     * @return true if retries are available, false otherwise
     */
    public boolean hasRetriesLeft() {
        return retryCount < maxRetries;
    }

    /**
     * Marks the event as complete.
     */
    public synchronized void markComplete() {
        complete = true;
    }
}
