package com.flipkart.varadhi.entities.cluster.failover;

import com.flipkart.varadhi.entities.RegionName;

/**
 * HTTP/body payload to trigger a topic failover to {@code targetRegion}.
 *
 * <p>{@code sourceRegion} may be omitted — the controller uses its deployed region. When present,
 * it must match the topic's current producing region.
 *
 * <p>{@code waitForReplicationLagToClear} (default {@code true}) holds MIGRATE until source→target
 * lag is clear. {@code lagDrainTimeoutMs} / {@code onLagTimeout} are optional lag-budget controls
 * (FAILOVER | ABORT); when null, controller defaults apply.
 *
 * <p>Caller identity is not part of this body — the web layer takes it from auth.
 */
public record TopicFailoverRequest(
    RegionName sourceRegion,
    RegionName targetRegion,
    boolean waitForReplicationLagToClear,
    Long lagDrainTimeoutMs,
    String onLagTimeout
) {
    public TopicFailoverRequest(RegionName sourceRegion, RegionName targetRegion, boolean waitForReplicationLagToClear) {
        this(sourceRegion, targetRegion, waitForReplicationLagToClear, null, null);
    }
}
