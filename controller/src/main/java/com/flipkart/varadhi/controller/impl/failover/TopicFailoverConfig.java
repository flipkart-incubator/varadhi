package com.flipkart.varadhi.controller.impl.failover;

/**
 * Controller-side timing for topic failover.
 *
 * <p>{@code stageAwaitMs} is the pod-ack barrier timeout for every stage (PREPARE / FENCE /
 * MIGRATE). {@code fenceHoldMs} is an optional pause after the FENCE barrier before MIGRATE
 * (harness/dev; {@code 0} in prod). Lag poll uses {@link #DRAIN_TIMEOUT_MS}.
 */
public record TopicFailoverConfig(long stageAwaitMs, long fenceHoldMs) {

    /** Max wait for optional replication-lag poll ({@code waitForReplicationLagToClear}). */
    public static final long DRAIN_TIMEOUT_MS = 300_000;

    public static TopicFailoverConfig defaultConfig() {
        return new TopicFailoverConfig(60_000, 0);
    }

    public long drainTimeoutMs() {
        return DRAIN_TIMEOUT_MS;
    }
}
