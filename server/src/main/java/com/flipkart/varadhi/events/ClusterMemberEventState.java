package com.flipkart.varadhi.events;

import com.flipkart.varadhi.core.cluster.entities.MemberInfo;
import lombok.Getter;

@Getter
public class ClusterMemberEventState {
    private final MemberInfo member;
    private final int maxRetries;
    private volatile int retryCount;
    private volatile boolean complete;

    public ClusterMemberEventState(MemberInfo member, int maxRetries) {
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must be non-negative");
        }
        this.member = member;
        this.maxRetries = maxRetries;
        this.retryCount = 0;
        this.complete = false;
    }

    public synchronized void incrementRetries() {
        retryCount++;
    }

    public boolean hasRetriesLeft() {
        return retryCount < maxRetries;
    }

    public synchronized void markComplete() {
        complete = true;
    }
}
