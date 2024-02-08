package com.flipkart.varadhi.consumer;

/**
 * Status for a consumer that has been started.
 */
public enum ConsumerStatus {

    CONSUMING,

    ERRORED,

    /**
     * Throttled is a wide bucket. TODO: evaluate splitting based on the requirements later on.
     */
    THROTTLED,

    PAUSED
}
