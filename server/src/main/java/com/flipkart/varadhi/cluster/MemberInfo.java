package com.flipkart.varadhi.cluster;

import com.flipkart.varadhi.components.ComponentKind;

/**
 * 'nodeId' is unique, and is the identifier for a node.
 * TODO::confirm if globally unique or with in a scope of a role.
 */
public record MemberInfo(
        String nodeId,
        String hostname,
        int port,
        ComponentKind[] roles,
        int cpuCount,
        int nicMBps
) {
}
