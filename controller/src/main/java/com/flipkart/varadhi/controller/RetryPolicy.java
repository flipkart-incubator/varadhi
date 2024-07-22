package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.entities.cluster.OrderedOperation;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class RetryPolicy {
    private final int maxRetryAllowed;
    private final int retryIntervalInSeconds;
    private final int minBackoffSeconds;
    private final int maxBackoffSeconds;


    public boolean canRetry(OrderedOperation operation) {
        // TODO::This needs better implementation, retry decision can also be impacted by kind of failure.
        return operation.hasFailed() && operation.getRetryAttempt() < maxRetryAllowed;
    }

    public int getRetryBackoffSeconds(OrderedOperation operation) {
        int retryAfter = operation.getRetryAttempt()*retryIntervalInSeconds;
        return Math.min(Math.max(minBackoffSeconds, retryAfter), maxBackoffSeconds);
    }
}
