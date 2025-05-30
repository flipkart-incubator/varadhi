package com.flipkart.varadhi.entities.auth;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.varadhi.entities.MetaStoreEntity;
import com.flipkart.varadhi.entities.MetaStoreEntityType;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Getter
@EqualsAndHashCode (callSuper = true)
public class IamPolicyRecord extends MetaStoreEntity {
    /**
     * Map of subject to roles
     */
    private final Map<String, Set<String>> roleBindings;

    @JsonCreator
    public IamPolicyRecord(
        @JsonProperty ("name") String name,
        @JsonProperty ("version") int version,
        @JsonProperty ("roleBindings") Map<String, Set<String>> roleBindings
    ) {
        super(name, version, MetaStoreEntityType.IAM_POLICY);
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
