package com.flipkart.varadhi.spi.db;

import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.entities.auth.RoleBindingNode;

import java.util.List;

public interface RoleBindingMetaStore {
    List<RoleBindingNode> getRoleBindingNodes();

    RoleBindingNode getRoleBindingNode(String resourceIdWithType);

    RoleBindingNode getRoleBindingNode(ResourceType resourceType, String resourceId);

    void createRoleBindingNode(RoleBindingNode node);

    boolean isRoleBindingNodePresent(ResourceType resourceType, String resourceId);

    int updateRoleBindingNode(RoleBindingNode node);

    void deleteRoleBindingNode(ResourceType resourceType, String resourceId);
}
