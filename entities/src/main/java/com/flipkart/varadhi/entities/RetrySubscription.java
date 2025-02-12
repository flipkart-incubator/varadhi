package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor (onConstructor_ = {@JsonCreator})
public class RetrySubscription {
    private final InternalCompositeSubscription[] retrySubscriptions;

    /**
     * @param retryCount 1-based retry count
     *
     * @return the storage subscription for the given retry count
     */

    @JsonIgnore
    public InternalCompositeSubscription getSubscriptionForRetry(int retryCount) {
        return retrySubscriptions[retryCount - 1];
    }

    @JsonIgnore
    public int getMaxRetryCount() {
        return retrySubscriptions.length;
    }
}
