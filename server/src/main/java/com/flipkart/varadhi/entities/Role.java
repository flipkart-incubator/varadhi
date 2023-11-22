package com.flipkart.varadhi.entities;

import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Set;

@Getter
@EqualsAndHashCode
public class Role {
    String roleId;
    Set<ResourceAction> permissions;

    public Role(String roleId, Set<ResourceAction> permissions) {
        this.roleId = roleId;
        this.permissions = permissions;
    }
}
