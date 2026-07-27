package com.flipkart.varadhi.controller.impl.failover;

/**
 * Controller-side timing for topic-failover stages. {@code prepareTimeoutMs} /
 * {@code switchTimeoutMs} bound pod-ack barriers; {@code drainTimeoutMs} bounds the
 * controller-side replication-lag poll when {@code waitForReplicationLagToClear} is set.
 */
public record TopicFailoverConfig(long prepareTimeoutMs, long switchTimeoutMs, long drainTimeoutMs) {

    public static TopicFailoverConfig defaultConfig() {
        return new TopicFailoverConfig(30_000, 60_000, 300_000);
    }
}
