package com.flipkart.varadhi.services;

import com.flipkart.varadhi.entities.auth.IAMPolicyRequest;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.entities.auth.RoleBindingNode;
import com.flipkart.varadhi.exceptions.IllegalArgumentException;
import com.flipkart.varadhi.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.spi.db.MetaStore;

import java.util.HashMap;
import java.util.List;

public class AuthZService {
    private final MetaStore metaStore;

    public AuthZService(MetaStore metaStore) {
        this.metaStore = metaStore;
    }

    public RoleBindingNode createRoleBindingNode(String resourceId, ResourceType resourceType) {
        if (!isResourceValid(resourceId, resourceType)) {
            throw new IllegalArgumentException(String.format(
                    "Invalid resource id(%s) for resource type(%s).",
                    resourceId,
                    resourceType
            ));
        }
        RoleBindingNode node = new RoleBindingNode(resourceId, resourceType, new HashMap<>(), 0);
        metaStore.createRoleBindingNode(node);
        return node;
    }

    public List<RoleBindingNode> getAllRoleBindingNodes() {
        return metaStore.getRoleBindingNodes();
    }

    public RoleBindingNode findRoleBindingNode(ResourceType resourceType, String resourceId) {
        return metaStore.getRoleBindingNode(resourceType, resourceId);
    }

    public RoleBindingNode updateRoleBindingNode(RoleBindingNode node) {
        boolean exists = metaStore.checkRoleBindingNodeExists(node.getResourceType(), node.getResourceId());
        if (!exists) {
            throw new ResourceNotFoundException(String.format(
                    "RoleBinding(%s) not found.",
                    node.getResourceId()
            ));
        }

        checkValidRoles(node);

        RoleBindingNode existingNode = metaStore.getRoleBindingNode(node.getResourceType(), node.getResourceId());
        if (node.getVersion() != existingNode.getVersion()) {
            throw new InvalidOperationForResourceException(String.format(
                    "Conflicting update, RoleBinding(%s) has been modified. Fetch latest and try again.",
                    node.getResourceId()
            ));
        }
        int updatedVersion = metaStore.updateRoleBindingNode(node);
        node.setVersion(updatedVersion);
        return node;
    }

    public RoleBindingNode getIAMPolicy(ResourceType resourceType, String resourceId) {
        return metaStore.getRoleBindingNode(resourceType, resourceId);
    }

    public RoleBindingNode setIAMPolicy(ResourceType resourceType, String resourceId, IAMPolicyRequest binding) {
        RoleBindingNode node = createOrGetRoleBindingNode(resourceId, resourceType);
        node.setRoleAssignment(binding.getSubject(), binding.getRoles());
        return updateRoleBindingNode(node);
    }

    public void deleteRoleBindingNode(ResourceType resourceType, String resourceId) {
        boolean exists = metaStore.checkRoleBindingNodeExists(resourceType, resourceId);
        if (!exists) {
            throw new ResourceNotFoundException(String.format(
                    "RoleBinding on resource(%s) not found.",
                    resourceId
            ));
        }
        metaStore.deleteRoleBindingNode(resourceType, resourceId);
    }

    private RoleBindingNode createOrGetRoleBindingNode(String resourceId, ResourceType resourceType) {
        boolean exists = metaStore.checkRoleBindingNodeExists(resourceType, resourceId);
        if (!exists) {
            return createRoleBindingNode(resourceId, resourceType);
        }
        RoleBindingNode existingNode = metaStore.getRoleBindingNode(resourceType, resourceId);
        if (existingNode.getResourceType() != resourceType) {
            throw new IllegalArgumentException(String.format(
                    "Incorrect resource type(%s) for resource id(%s).",
                    resourceType,
                    resourceId
            ));
        }
        return existingNode;
    }

    private boolean isResourceValid(String resourceId, ResourceType resourceType) {
        return switch (resourceType) {
            case ORG -> metaStore.checkOrgExists(resourceId);
            case TEAM -> {
                // org:team
                String[] segments = resourceId.split(":");
                yield (segments.length == 2) && metaStore.checkTeamExists(segments[1], segments[0]);
            }
            case PROJECT -> metaStore.checkProjectExists(resourceId);
            case TOPIC -> {
                // project:topic
                String[] segments = resourceId.split(":");
                yield (segments.length == 2) && metaStore.checkTopicResourceExists(segments[1], segments[0]);
            }
            case SUBSCRIPTION -> false; //TODO
        };
    }

    private void checkValidRoles(RoleBindingNode node) {
        // collect roles for each subject into a single set
        // TODO: check valid roleId?
    }
}
