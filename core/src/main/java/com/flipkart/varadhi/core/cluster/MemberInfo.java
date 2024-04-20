package com.flipkart.varadhi.core.cluster;


public record MemberInfo(
        String hostname,
        int port,
        ComponentKind[] roles,
        int cpuCount,
        int nicMBps
) {
}
