package com.flipkart.varadhi.entities.auth;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.varadhi.entities.ValidateResource;
import com.flipkart.varadhi.entities.MetaStoreEntity;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Getter
@EqualsAndHashCode(callSuper = true)
@ValidateResource(message = "Invalid Role Binding name. Check naming constraints.")
public class RoleBindingNode extends MetaStoreEntity {
    @NotBlank
    private final String resourceId;

    @NotNull
    private final ResourceType resourceType;

    /**
     * Map of subject to roles
     */
    @NotNull
    private final Map<String, Set<String>> rolesAssignment;

    @JsonCreator
    public RoleBindingNode(
            @JsonProperty("resourceId") String resourceId,
            @JsonProperty("resourceType") ResourceType resourceType,
            @JsonProperty("rolesAssignment")
            Map<String, Set<String>> rolesAssignment,
            @JsonProperty("version")
            int version
    ) {
        super(resourceId, version);
        this.resourceId = resourceId;
        this.resourceType = resourceType;
        this.rolesAssignment = new HashMap<>();
        this.rolesAssignment.putAll(rolesAssignment);
    }

    public void setRoleAssignment(String subject, Set<String> roles) {
        if (roles.isEmpty()) {
            rolesAssignment.remove(subject);
            return;
        }
        rolesAssignment.put(subject, roles);
    }
}
