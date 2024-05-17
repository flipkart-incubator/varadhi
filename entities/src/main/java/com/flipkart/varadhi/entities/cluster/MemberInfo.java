package com.flipkart.varadhi.entities.cluster;


public record MemberInfo(
        String hostname,
        int port,
        ComponentKind[] roles,
        MemberResources capacity
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
