package com.flipkart.varadhi.utils;

import com.flipkart.varadhi.entities.auth.IamPolicyRecord;
import com.flipkart.varadhi.entities.auth.IamPolicyResponse;
import com.flipkart.varadhi.entities.auth.ResourceType;

public final class IamPolicyHelper {
    private IamPolicyHelper() {
    }

    public static final String AUTH_RESOURCE_NAME_SEPARATOR = ":";

    public static IamPolicyResponse toResponse(IamPolicyRecord policy) {
        return new IamPolicyResponse(
                policy.getName(),
                policy.getRoleBindings(),
                policy.getVersion()
        );
    }

    public static String getAuthResourceFQN(ResourceType resourceType, String resourceId) {
        return String.join(AUTH_RESOURCE_NAME_SEPARATOR, resourceType.name(), resourceId);
    }
}
