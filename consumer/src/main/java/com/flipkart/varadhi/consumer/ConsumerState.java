package com.flipkart.varadhi.consumer;

/**
 * Status for a consumer that has been started.
 */
public enum ConsumerState {

    /**
     * varadhi consumer will try to make delivery based on the subscription details.
     */
    CONSUMING,

    /**
     * varadhi consumer has trapped an uncaught exception and is haulted.
     */
    ERRORED,

    /**
     * varadhi consumer is delivering at throttled rate, lost likely due to destination errors and subscription's
     * consumption policy.
     * Throttled is a wide bucket. TODO: evaluate splitting based on the requirements later on.
     */
    THROTTLED,

    /**
     * varadhi consumer is paused and not making message delivery. But it <i>may</i> be processing failed groups.
     */
    PAUSED
}
