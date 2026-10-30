package com.flipkart.varadhi.entities.cluster.transition.failover;

import com.flipkart.varadhi.entities.RegionName;

/**
 * HTTP/body payload to trigger a topic failover to {@code targetRegion}.
 *
 * <p>{@code sourceRegion} may be omitted — the REST handler defaults it to the deployed region
 * (the controller does the same if the RPC still omits it). Both regions must be registered and
 * configured on the topic, must differ, and source must be producing.
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
}
