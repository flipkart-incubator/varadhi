package com.flipkart.varadhi.core.cluster.entities;


public record MemberInfo(String hostname, String address, int port, ComponentKind[] roles,
                         NodeCapacity provisionedCapacity) {
    public boolean hasRole(ComponentKind role) {
        for (ComponentKind r : roles) {
            if (r == role) {
                return true;
            }
        }
        return false;
    }
}
