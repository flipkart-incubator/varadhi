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
        int maxAllowed = operation.maxRetryAllowed(maxRetryAllowed);
        return operation.hasFailed() && operation.getRetryAttempt() < maxAllowed;
    }

    public int getRetryBackoffSeconds(OrderedOperation operation) {
        int retryAfter = operation.getRetryAttempt() * retryIntervalInSeconds;
        return Math.min(Math.max(minBackoffSeconds, retryAfter), maxBackoffSeconds);
    }
}
