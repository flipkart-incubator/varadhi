package com.flipkart.varadhi.utils;

import com.flipkart.varadhi.entities.auth.IAMPolicyRecord;
import com.flipkart.varadhi.entities.auth.IAMPolicyResponse;
import com.flipkart.varadhi.entities.auth.ResourceType;

public final class AuthZHelper {
    private AuthZHelper() {
    }

    public static final String AUTH_RESOURCE_NAME_SEPARATOR = ":";

    public static IAMPolicyResponse toResponse(IAMPolicyRecord policy) {
        String[] segments = policy.getAuthResourceId().split(AUTH_RESOURCE_NAME_SEPARATOR, 2);
        return new IAMPolicyResponse(
                policy.getName(),
                segments[1],
                ResourceType.valueOf(segments[0]),
                policy.getRoleBindings(),
                policy.getVersion()
        );
    }

    public static String getAuthResourceFQN(ResourceType resourceType, String resourceId) {
        return String.join(AUTH_RESOURCE_NAME_SEPARATOR, resourceType.name(), resourceId);
    }
}
