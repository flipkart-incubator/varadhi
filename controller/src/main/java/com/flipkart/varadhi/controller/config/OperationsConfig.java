package com.flipkart.varadhi.controller.config;

import lombok.Data;

@Data
public class OperationsConfig {
    private int maxConcurrentOps = 2;
    private int maxRetryAllowed = 3;
    /** Retries for {@link com.flipkart.varadhi.entities.cluster.TopicFailoverOperation} only; {@code 0} = fail once. */
    private int topicFailoverMaxRetryAllowed = 0;
    private int retryIntervalInSeconds = 10;
    private int retryMinBackoffInSeconds = 10;
    private int retryMaxBackOffInSeconds = 60;
}
