package com.flipkart.varadhi.entities.auth;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.varadhi.entities.ValidateVaradhiResource;
import com.flipkart.varadhi.entities.VaradhiResource;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Getter
@EqualsAndHashCode(callSuper = true)
@ValidateVaradhiResource(message = "Invalid Role Binding name. Check naming constraints.")
public class RoleBindingNode extends VaradhiResource {
    @NotBlank
    private final String resourceId;

    @NotNull
    private final ResourceType resourceType;

    @NotNull
    private final Map<String, Set<String>> subjectToRolesMapping;

    @JsonCreator
    public RoleBindingNode(
            @JsonProperty("resourceId") String resourceId,
            @JsonProperty("resourceType") ResourceType resourceType,
            @JsonProperty("subjectToRolesMapping")
            Map<String, Set<String>> subjectToRolesMapping,
            @JsonProperty("version")
            int version
    ) {
        super(resourceId, version);
        this.resourceId = resourceId;
        this.resourceType = resourceType;
        this.subjectToRolesMapping = new HashMap<>();
        this.subjectToRolesMapping.putAll(subjectToRolesMapping);
    }

    public void setRoleAssignment(String subject, Set<String> roles) {
        if (roles.isEmpty()) {
            subjectToRolesMapping.remove(subject);
            return;
        }
        subjectToRolesMapping.put(subject, roles);
    }
}
