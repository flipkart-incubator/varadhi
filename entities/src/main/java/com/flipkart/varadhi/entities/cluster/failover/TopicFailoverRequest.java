package com.flipkart.varadhi.entities.cluster.failover;

import com.flipkart.varadhi.entities.RegionName;

/**
 * HTTP/body payload to trigger a topic failover: move produce from {@code sourceRegion} to
 * {@code targetRegion}. {@code waitForReplicationLagToClear} optionally holds the transition in
 * DRAIN (after PREPARE, before SWITCH) until source→target replication lag is clear.
 *
 * <p>{@code sourceRegion} is required (not inferred) so the caller states the failover direction
 * explicitly; the controller validates it matches the topic's current producing region, which
 * catches a stale client racing a concurrent failover and prevents an accidental wrong-way switch.
 *
 * <p>Caller identity is not part of this body — the web layer takes it from auth and passes it as a
 * separate {@code requestedBy} argument (same pattern as abort / subscription ops).
 */
public record TopicFailoverRequest(
    RegionName sourceRegion,
    RegionName targetRegion,
    boolean waitForReplicationLagToClear
) {
}
