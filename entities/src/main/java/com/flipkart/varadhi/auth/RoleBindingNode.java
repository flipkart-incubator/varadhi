package com.flipkart.varadhi.auth;

import com.flipkart.varadhi.ValidateVaradhiResource;
import com.flipkart.varadhi.entities.ResourceType;
import com.flipkart.varadhi.entities.VaradhiResource;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Getter
@EqualsAndHashCode(callSuper = true)
@ValidateVaradhiResource(message = "Invalid Role Binding name. Check naming constraints.")
public class RoleBindingNode extends VaradhiResource {
    String resourceId;
    ResourceType resourceType;
    Map<String, Set<String>> subjectToRolesMapping;

    public RoleBindingNode(
            String resourceId, ResourceType resourceType, Map<String, Set<String>> subjectToRolesMapping, int version
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
