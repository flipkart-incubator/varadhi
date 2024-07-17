package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.entities.cluster.OrderedOperation;

public class RetryPolicy {
    private final int maxRetryAllowed = 2;
    private final int minBackoffSeconds = 1;
    private final int maxBackoffSeconds = 10;

    public boolean canRetry(OrderedOperation operation) {
        // TODO::This needs better implementation, retry decision can also be impacted by kind of failure.
        return operation.hasFailed() && operation.getRetryAttempt() < maxRetryAllowed;
    }

    public int getRetryBackoffSeconds(OrderedOperation operation) {
        return maxBackoffSeconds;
    }
}
