package com.flipkart.varadhi.entities;

import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Set;

@Getter
@EqualsAndHashCode
public class RoleAssignmentUpdate {
    String resourceId;
    ResourceType resourceType;
    String subject;
    Set<String> roles;

    public RoleAssignmentUpdate(String resourceId, ResourceType resourceType, String subject, Set<String> roles) {
        this.resourceId = resourceId;
        this.resourceType = resourceType;
        this.subject = subject;
        this.roles = roles;
    }
}
