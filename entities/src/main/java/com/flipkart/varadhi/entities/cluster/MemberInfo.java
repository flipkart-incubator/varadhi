package com.flipkart.varadhi.entities.cluster;


public record MemberInfo(
        String host,
        int port,
        ComponentKind[] roles,
        NodeCapacity provisionedCapacity
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
