package com.flipkart.varadhi.entities.cluster;

public interface OrderedOperation extends Operation {
    String getOrderingKey();

    int getRetryAttempt();

    OrderedOperation nextRetry();

    /**
     * Max retries allowed for this operation. Defaults to the policy-wide {@code policyDefault};
     * override to stamp a type-specific limit (e.g. {@link TopicFailoverOperation}) so
     * {@code controller.RetryPolicy} does not need to know about concrete operation types.
     */
    default int maxRetryAllowed(int policyDefault) {
        return policyDefault;
    }
}
