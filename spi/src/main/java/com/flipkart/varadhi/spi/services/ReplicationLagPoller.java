package com.flipkart.varadhi.spi.services;

import java.util.concurrent.CompletableFuture;

/**
 * SPI implemented per storage backend (Pulsar, Kafka, …) to expose the per-region
 * replication lag for a topic during the DRAIN stage of a topic failover.
 *
 * <p>The controller polls {@link #pollLagMs(String, String)} on a fixed interval
 * until the value reaches {@code 0} (or the per-op deadline expires). No pod is
 * involved in DRAIN; this call goes directly from the controller to the storage
 * broker (or its admin API).
 */
public interface ReplicationLagPoller {

    /**
     * Returns the current replication lag, in milliseconds, of {@code topicFqn}
     * from {@code sourceRegion} to the corresponding target topic that the failover
     * will switch traffic to. Implementations should return a small positive value
     * while replication is still catching up, and {@code 0} when the target is
     * fully caught up. {@code -1} signals "lag is unknown" and the caller treats it
     * the same as a positive value for control-flow purposes.
     */
    CompletableFuture<Long> pollLagMs(String topicFqn, String sourceRegion);

    /**
     * Default no-op implementation: assumes lag is always {@code 0}. Useful for
     * tests / dev mode and as the default binding until a per-broker implementation
     * is plugged in.
     */
    ReplicationLagPoller NO_OP = (topic, region) -> CompletableFuture.completedFuture(0L);
}
