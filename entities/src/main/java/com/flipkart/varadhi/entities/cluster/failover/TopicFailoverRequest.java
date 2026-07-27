package com.flipkart.varadhi.entities.cluster.failover;

import com.flipkart.varadhi.entities.RegionName;

/**
 * Request payload to trigger a topic failover: move produce from {@code sourceRegion} to
 * {@code targetRegion}. {@code waitForReplicationLagToClear} optionally holds the transition in
 * DRAIN until the source's replication backlog is fully consumed before completing.
 *
 * <p>{@code sourceRegion} is required (not inferred) so the caller states the failover direction
 * explicitly; the controller validates it matches the topic's current producing region, which
 * catches a stale client racing a concurrent failover and prevents an accidental wrong-way switch.
 *
 * <p>{@code requestedBy} is stamped server-side from the caller identity (not from the client body,
 * which cannot be trusted for audit) and is carried on this model so it flows straight into the
 * durable {@code TopicFailoverOperation} record in the {@code OpStore} for audit.
 */
public record TopicFailoverRequest(
    RegionName sourceRegion,
    RegionName targetRegion,
    boolean waitForReplicationLagToClear,
    String requestedBy
) {
    public TopicFailoverRequest withRequestedBy(String identity) {
        return new TopicFailoverRequest(sourceRegion, targetRegion, waitForReplicationLagToClear, identity);
    }
}
