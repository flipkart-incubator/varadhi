package com.flipkart.varadhi.auth;

public record PermissionAuthorization(ResourceAction action, ResourceName resource) {

    public static PermissionAuthorization of(ResourceAction action, String resource) {
        return new PermissionAuthorization(action, new ResourceName(resource));
    }
}
