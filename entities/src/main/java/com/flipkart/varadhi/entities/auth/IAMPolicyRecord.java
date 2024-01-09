package com.flipkart.varadhi.entities.auth;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.varadhi.entities.MetaStoreEntity;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Getter
@EqualsAndHashCode(callSuper = true)
public class IAMPolicyRecord extends MetaStoreEntity {
    private final String resourceId;
    private final ResourceType resourceType;
    /**
     * Map of subject to roles
     */
    private final Map<String, Set<String>> roleBindings;

    @JsonCreator
    public IAMPolicyRecord(
            @JsonProperty("resourceId") String resourceId,
            @JsonProperty("resourceType") ResourceType resourceType,
            @JsonProperty("roleBindings")
            Map<String, Set<String>> roleBindings,
            @JsonProperty("version")
            int version
    ) {
        super(resourceId, version);
        this.resourceId = resourceId;
        this.resourceType = resourceType;
        this.roleBindings = new HashMap<>();
        this.roleBindings.putAll(roleBindings);
    }

    public void setRoleAssignment(String subject, Set<String> roles) {
        if (roles.isEmpty()) {
            roleBindings.remove(subject);
            return;
        }
        roleBindings.put(subject, roles);
    }
}
