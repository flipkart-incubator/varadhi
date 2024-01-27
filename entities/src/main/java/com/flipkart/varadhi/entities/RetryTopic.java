package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * TODO: generalize the retryTopic interaction. Currently, we assume that there is a retry topic per retry,
 * but pulsar has delayed topics, which needs evaluation. But maybe that is not a concern for this class. This class is
 * just one impl of RetryTopic, and there may be other impls.
 */
public class RetryTopic {

    private final StorageTopic[] retryTopics;

    public RetryTopic(StorageTopic[] retryTopics) {
        this.retryTopics = retryTopics;
    }

    /**
     * @param retryCount 1-based retry count
     *
     * @return the storage topic for the given retry count
     */
    @JsonIgnore
    public StorageTopic getTopicForRetry(int retryCount) {
        return retryTopics[retryCount - 1];
    }

    @JsonIgnore
    public int getMaxRetryCount() {
        return retryTopics.length;
    }
}
