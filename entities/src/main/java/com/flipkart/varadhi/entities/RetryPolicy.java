package com.flipkart.varadhi.entities;


import lombok.Data;

@Data
public class RetryPolicy {
    private final CodeRange[] retryCodes;
    private final BackoffType backoffType;
    private final int minBackoff;
    private final int maxBackoff;
    private final int multiplier;
    private final int retryAttempts;

    public enum BackoffType {
        LINEAR,
        EXPONENTIAL
    }
}
