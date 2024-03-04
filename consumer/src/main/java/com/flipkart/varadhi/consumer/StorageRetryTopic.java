package com.flipkart.varadhi.consumer;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.flipkart.varadhi.entities.StorageTopic;

public class StorageRetryTopic {

    private final StorageTopic[] retryTopics;

    public StorageRetryTopic(StorageTopic[] retryTopics) {
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
