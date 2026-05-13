package com.flipkart.varadhi.core.cluster;

import com.flipkart.varadhi.entities.RegionName;

/**
 * Immutable snapshot of a cluster member's identity and capabilities.
 * <p>
 * {@code region} identifies the deployment region of this node and is used for multi-region topology,
 * region-aware routing, and failover decisions.
 */
public record MemberInfo(
    String hostname,
    String address,
    int port,
    ComponentKind[] roles,
    NodeCapacity provisionedCapacity,
    RegionName region
) {
    public boolean hasRole(ComponentKind role) {
        for (ComponentKind r : roles) {
            if (r == role) {
                return true;
            }
        }
        return false;
    }
}
