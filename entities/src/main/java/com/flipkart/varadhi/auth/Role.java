package com.flipkart.varadhi.auth;

import com.flipkart.varadhi.ValidateVaradhiResource;
import com.flipkart.varadhi.entities.VaradhiResource;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.List;

@Getter
@EqualsAndHashCode(callSuper = true)
@ValidateVaradhiResource(message = "Invalid Role name. Check naming constraints.")
public class Role extends VaradhiResource {
    String roleId;
    List<ResourceAction> permissions;

    public Role(String roleId, int version, List<ResourceAction> permissions) {
        super(roleId, version);
        this.roleId = roleId;
        this.permissions = permissions;
    }
}
