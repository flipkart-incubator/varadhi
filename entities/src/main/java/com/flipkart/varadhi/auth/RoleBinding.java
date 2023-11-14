package com.flipkart.varadhi.auth;

import com.flipkart.varadhi.ValidateVaradhiResource;
import com.flipkart.varadhi.entities.VaradhiResource;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.List;
import java.util.Map;

@Getter
@EqualsAndHashCode(callSuper = true)
@ValidateVaradhiResource(message = "Invalid Role Binding name. Check naming constraints.")
public class RoleBinding extends VaradhiResource {
    String resourceId;
    ResourceType resourceType;
    Map<String, List<String>> userToRoleBindings;

    protected RoleBinding(
            String resourceId, ResourceType resourceType, Map<String, List<String>> userToRoleBindings, int version
    ) {
        super(resourceId, version);
        this.resourceId = resourceId;
        this.resourceType = resourceType;
        this.userToRoleBindings = userToRoleBindings;
    }
}
