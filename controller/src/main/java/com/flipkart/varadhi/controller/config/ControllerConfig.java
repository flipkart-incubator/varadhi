package com.flipkart.varadhi.controller.config;

import lombok.Data;

@Data
public class ControllerConfig {
    private int maxConcurrentOps = 2;
    private int maxRetryAllowed = 3;
    private int retryIntervalInSeconds = 10;
    private int retryMinBackoffInSeconds = 10;
    private int retryMaxBackOffInSeconds = 60;
}
