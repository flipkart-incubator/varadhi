package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * An implementation of RetryTopic where there is one topic per retry.
 */
public class RetryTopic {

    private final InternalCompositeTopic[] retryTopics;

    public RetryTopic(InternalCompositeTopic[] retryTopics) {
        this.retryTopics = retryTopics;
    }

    /**
     * @param retryCount 1-based retry count
     *
     * @return the storage topic for the given retry count
     */
    @JsonIgnore
    public InternalCompositeTopic getTopicForRetry(int retryCount) {
        return retryTopics[retryCount - 1];
    }

    @JsonIgnore
    public int getMaxRetryCount() {
        return retryTopics.length;
    }
}
