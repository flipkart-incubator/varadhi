package com.flipkart.varadhi.controller.impl.failover;

/**
 * Controller-side timing for the topic-failover stage barriers. Each value bounds how long the
 * controller waits for all expected pods to ack a stage before failing the stage (which aborts a
 * pre-switch transition or fails a post-switch one).
 */
public record TopicFailoverConfig(long prepareTimeoutMs, long switchTimeoutMs, long drainTimeoutMs) {

    public static TopicFailoverConfig defaultConfig() {
        return new TopicFailoverConfig(30_000, 60_000, 300_000);
    }
}
