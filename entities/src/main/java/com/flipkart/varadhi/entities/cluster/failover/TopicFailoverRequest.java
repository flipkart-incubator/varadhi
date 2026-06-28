package com.flipkart.varadhi.entities.cluster.failover;

import com.flipkart.varadhi.entities.RegionName;

/**
 * Request payload to trigger a topic failover: move produce from {@code sourceRegion} to
 * {@code targetRegion}. {@code waitForReplicationLagToClear} optionally holds the transition in
 * DRAIN until the source's replication backlog is fully consumed before completing.
 *
 * <p>{@code requestedBy} is stamped server-side from the caller identity (not from the client body).
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
