package com.flipkart.varadhi.entities.auth;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.varadhi.entities.VersionedEntity;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Map;
import java.util.Set;

@Getter
@EqualsAndHashCode(callSuper = true)
public class IamPolicyResponse extends VersionedEntity {
    private final String resourceId;
    private final ResourceType resourceType;
    /**
     * Map of subject to roles
     */
    private final Map<String, Set<String>> roleBindings;

    @JsonCreator
    public IamPolicyResponse(
            @JsonProperty("name") String name,
            @JsonProperty("resourceId") String resourceId,
            @JsonProperty("resourceType") ResourceType resourceType,
            @JsonProperty("roleBindings")
            Map<String, Set<String>> roleBindings,
            @JsonProperty("version") int version
    ) {
        super(name, version);
        this.resourceId = resourceId;
        this.resourceType = resourceType;
        this.roleBindings = roleBindings;
    }
}
