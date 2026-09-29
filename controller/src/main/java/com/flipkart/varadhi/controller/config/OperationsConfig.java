package com.flipkart.varadhi.controller.config;

import lombok.Data;

@Data
public class OperationsConfig {
    private int maxConcurrentOps = 2;
    private int maxRetryAllowed = 3;
    /**
     * Max retries for topic-failover ops only ({@code 0} = fail once). Used to build a separate
     * {@link com.flipkart.varadhi.controller.RetryPolicy} when enqueueing failover — same backoff
     * knobs as {@link #maxRetryAllowed}, different ceiling.
     */
    private int topicFailoverMaxRetryAllowed = 0;
    private int retryIntervalInSeconds = 10;
    private int retryMinBackoffInSeconds = 10;
    private int retryMaxBackOffInSeconds = 60;
    /** Pod-ack barrier timeout for PREPARE / FENCE / MIGRATE. */
    private long topicFailoverStageAwaitMs = 60_000;
    /**
     * Optional pause after FENCE before MIGRATE ({@code 0} = none). Harness/dev only.
     */
    private long topicFailoverFenceHoldMs = 0;
}
