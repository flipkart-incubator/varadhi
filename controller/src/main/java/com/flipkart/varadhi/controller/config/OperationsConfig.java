package com.flipkart.varadhi.controller.config;

import lombok.Data;

@Data
public class OperationsConfig {
    private int maxConcurrentOps = 2;
    private int maxRetryAllowed = 3;
    /**
     * Retries for {@link com.flipkart.varadhi.entities.cluster.TopicFailoverOperation} only;
     * {@code 0} = fail once. Kept separate from {@link #maxRetryAllowed} because a stuck failover
     * has different retry economics than a routine subscription operation (see
     * {@link com.flipkart.varadhi.controller.impl.failover.TopicFailoverConfig} for the related
     * per-stage timeouts). Stamped onto each {@code TopicFailoverOperation} at creation time.
     */
    private int topicFailoverMaxRetryAllowed = 0;
    private int retryIntervalInSeconds = 10;
    private int retryMinBackoffInSeconds = 10;
    private int retryMaxBackOffInSeconds = 60;
}
