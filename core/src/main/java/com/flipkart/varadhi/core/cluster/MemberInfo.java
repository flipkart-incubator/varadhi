package com.flipkart.varadhi.core.cluster;


/**
 * 'memberId' is unique, and is the identifier for a node.
 * TODO::confirm if globally unique or with in a scope of a role.
 */
public record MemberInfo(
        String memberId,
        String hostname,
        int port,
        ComponentKind[] roles,
        int cpuCount,
        int nicMBps
) {
}
