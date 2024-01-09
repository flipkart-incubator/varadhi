package com.flipkart.varadhi.services;

import com.flipkart.varadhi.entities.auth.IAMPolicyRequest;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.entities.auth.RoleBindingNode;
import com.flipkart.varadhi.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.db.RoleBindingMetaStore;

import java.util.HashMap;
import java.util.List;

import static com.flipkart.varadhi.entities.VersionedEntity.NAME_SEPARATOR;

public class AuthZService {
    private final MetaStore metaStore;
    private final RoleBindingMetaStore roleBindingMetaStore;

    public AuthZService(MetaStore metaStore, RoleBindingMetaStore roleBindingMetaStore) {
        this.metaStore = metaStore;
        this.roleBindingMetaStore = roleBindingMetaStore;
    }

    public RoleBindingNode createRoleBindingNode(String resourceId, ResourceType resourceType) {
        if (!isResourceValid(resourceId, resourceType)) {
            throw new ResourceNotFoundException(
                    "RoleBinding on resource (%s:%s) not found.".formatted(resourceType, resourceId));
        }
        RoleBindingNode node = new RoleBindingNode(resourceId, resourceType, new HashMap<>(), 0);
        roleBindingMetaStore.createRoleBindingNode(node);
        return node;
    }

    public List<RoleBindingNode> getAllRoleBindingNodes() {
        return roleBindingMetaStore.getRoleBindingNodes();
    }

    public RoleBindingNode getRoleBindingNode(ResourceType resourceType, String resourceId) {
        return roleBindingMetaStore.getRoleBindingNode(resourceType, resourceId);
    }

    public RoleBindingNode updateRoleBindingNode(RoleBindingNode node) {
        boolean exists = roleBindingMetaStore.isRoleBindingNodePresent(node.getResourceType(), node.getResourceId());
        if (!exists) {
            throw new ResourceNotFoundException(String.format(
                    "RoleBinding(%s) not found.",
                    node.getResourceId()
            ));
        }

        RoleBindingNode existingNode =
                roleBindingMetaStore.getRoleBindingNode(node.getResourceType(), node.getResourceId());
        if (node.getVersion() != existingNode.getVersion()) {
            throw new InvalidOperationForResourceException(String.format(
                    "Conflicting update, RoleBinding(%s) has been modified. Fetch latest and try again.",
                    node.getResourceId()
            ));
        }
        int updatedVersion = roleBindingMetaStore.updateRoleBindingNode(node);
        node.setVersion(updatedVersion);
        return node;
    }

    public RoleBindingNode getIAMPolicy(ResourceType resourceType, String resourceId) {
        return roleBindingMetaStore.getRoleBindingNode(resourceType, resourceId);
    }

    public RoleBindingNode setIAMPolicy(ResourceType resourceType, String resourceId, IAMPolicyRequest binding) {
        RoleBindingNode node = createOrGetRoleBindingNode(resourceId, resourceType);
        node.setRoleAssignment(binding.getSubject(), binding.getRoles());
        return updateRoleBindingNode(node);
    }

    public void deleteRoleBindingNode(ResourceType resourceType, String resourceId) {
        boolean exists = roleBindingMetaStore.isRoleBindingNodePresent(resourceType, resourceId);
        if (!exists) {
            throw new ResourceNotFoundException(String.format(
                    "RoleBinding on resource(%s) not found.",
                    resourceId
            ));
        }
        roleBindingMetaStore.deleteRoleBindingNode(resourceType, resourceId);
    }

    private RoleBindingNode createOrGetRoleBindingNode(String resourceId, ResourceType resourceType) {
        boolean exists = roleBindingMetaStore.isRoleBindingNodePresent(resourceType, resourceId);
        if (!exists) {
            return createRoleBindingNode(resourceId, resourceType);
        }
        return roleBindingMetaStore.getRoleBindingNode(resourceType, resourceId);
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
                String varadhiTopicName = String.join(NAME_SEPARATOR, segments[0], segments[1]);
                yield (segments.length == 2) && metaStore.checkTopicExists(varadhiTopicName);
            }
            case SUBSCRIPTION -> false; //TODO
        };
    }
}
