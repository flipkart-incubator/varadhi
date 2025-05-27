package com.flipkart.varadhi.utils;

import com.flipkart.varadhi.entities.auth.IamPolicyRecord;
import com.flipkart.varadhi.entities.auth.IamPolicyResponse;
import com.flipkart.varadhi.entities.auth.EntityType;

public final class IamPolicyHelper {
    private IamPolicyHelper() {
    }

    public static final String AUTH_RESOURCE_NAME_SEPARATOR = ":";

    public static IamPolicyResponse toResponse(IamPolicyRecord policy) {
        return new IamPolicyResponse(policy.getName(), policy.getRoleBindings(), policy.getVersion());
    }

    public static String getAuthResourceFQN(EntityType entityType, String resourceId) {
        return String.join(AUTH_RESOURCE_NAME_SEPARATOR, entityType.name(), resourceId);
    }
}
