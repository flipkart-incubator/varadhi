package com.flipkart.varadhi.services;

import com.flipkart.varadhi.auth.Role;
import com.flipkart.varadhi.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.spi.db.MetaStore;

import java.util.List;

public class DefaultAuthZService {
    private final MetaStore metaStore;

    public DefaultAuthZService(MetaStore metaStore) {
        this.metaStore = metaStore;
    }

    public Role createRole(Role role) {
        metaStore.createRole(role);
        return role;
    }

    public List<Role> getRoles() {
        return metaStore.getRoles();
    }

    public Role getRole(String roleName) {
        return metaStore.getRole(roleName);
    }

    public Role updateRole(Role role) {
        boolean roleExists = metaStore.checkRoleExists(role.getName());
        if (!roleExists) {
            throw new ResourceNotFoundException(String.format(
                    "Role(%s) not found.",
                    role.getName()
            ));
        }

        Role existingRole = metaStore.getRole(role.getName());
        if (role.getVersion() != existingRole.getVersion()) {
            throw new InvalidOperationForResourceException(String.format(
                    "Conflicting update, Role(%s) has been modified. Fetch latest and try again.", role.getName()
            ));
        }
        int updatedVersion = metaStore.updateRole(role);
        role.setVersion(updatedVersion);
        return role;
    }

    public void deleteRole(String roleName) {
        boolean roleExists = metaStore.checkRoleExists(roleName);
        if (!roleExists) {
            throw new ResourceNotFoundException(String.format(
                    "Role(%s) not found.",
                    roleName
            ));
        }
        metaStore.deleteRole(roleName);
    }
}
