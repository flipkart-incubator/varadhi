package com.flipkart.varadhi.services;

import com.flipkart.varadhi.entities.auth.IAMPolicyRecord;
import com.flipkart.varadhi.entities.auth.IAMPolicyRequest;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.spi.db.IAMPolicyMetaStore;
import com.flipkart.varadhi.spi.db.MetaStore;

import java.util.HashMap;
import java.util.List;

import static com.flipkart.varadhi.entities.VersionedEntity.NAME_SEPARATOR;

public class AuthZService {
    private final MetaStore metaStore;
    private final IAMPolicyMetaStore iamPolicyMetaStore;

    public AuthZService(MetaStore metaStore, IAMPolicyMetaStore iamPolicyMetaStore) {
        this.metaStore = metaStore;
        this.iamPolicyMetaStore = iamPolicyMetaStore;
    }

    public IAMPolicyRecord createIAMPolicyRecord(String resourceId, ResourceType resourceType) {
        if (!isResourceValid(resourceId, resourceType)) {
            throw new IllegalArgumentException(
                    "Invalid resource id(%s) for resource type(%s).".formatted(resourceId, resourceType));
        }
        IAMPolicyRecord node = new IAMPolicyRecord(resourceId, resourceType, new HashMap<>(), 0);
        iamPolicyMetaStore.createIAMPolicyRecord(node);
        return node;
    }

    public List<IAMPolicyRecord> getAllIAMPolicyRecords() {
        return iamPolicyMetaStore.getIAMPolicyRecords();
    }

    public IAMPolicyRecord getIAMPolicyRecord(ResourceType resourceType, String resourceId) {
        return iamPolicyMetaStore.getIAMPolicyRecord(resourceType, resourceId);
    }

    public IAMPolicyRecord updateIAMPolicyRecord(IAMPolicyRecord node) {
        boolean exists = iamPolicyMetaStore.isIAMPolicyRecordPresent(node.getResourceType(), node.getResourceId());
        if (!exists) {
            throw new ResourceNotFoundException(String.format(
                    "IAMPolicyRecord(%s) not found.",
                    node.getResourceId()
            ));
        }

        IAMPolicyRecord existingNode =
                iamPolicyMetaStore.getIAMPolicyRecord(node.getResourceType(), node.getResourceId());
        if (node.getVersion() != existingNode.getVersion()) {
            throw new InvalidOperationForResourceException(String.format(
                    "Conflicting update, IAMPolicyRecord(%s) has been modified. Fetch latest and try again.",
                    node.getResourceId()
            ));
        }
        int updatedVersion = iamPolicyMetaStore.updateIAMPolicyRecord(node);
        node.setVersion(updatedVersion);
        return node;
    }

    public IAMPolicyRecord getIAMPolicy(ResourceType resourceType, String resourceId) {
        return iamPolicyMetaStore.getIAMPolicyRecord(resourceType, resourceId);
    }

    public IAMPolicyRecord setIAMPolicy(ResourceType resourceType, String resourceId, IAMPolicyRequest binding) {
        IAMPolicyRecord node = createOrGetIAMPolicyRecord(resourceId, resourceType);
        node.setRoleAssignment(binding.getSubject(), binding.getRoles());
        return updateIAMPolicyRecord(node);
    }

    public void deleteIAMPolicyRecord(ResourceType resourceType, String resourceId) {
        boolean exists = iamPolicyMetaStore.isIAMPolicyRecordPresent(resourceType, resourceId);
        if (!exists) {
            throw new ResourceNotFoundException(String.format(
                    "IAM Policy Record on resource(%s) not found.",
                    resourceId
            ));
        }
        iamPolicyMetaStore.deleteIAMPolicyRecord(resourceType, resourceId);
    }

    private IAMPolicyRecord createOrGetIAMPolicyRecord(String resourceId, ResourceType resourceType) {
        boolean exists = iamPolicyMetaStore.isIAMPolicyRecordPresent(resourceType, resourceId);
        if (!exists) {
            return createIAMPolicyRecord(resourceId, resourceType);
        }
        return iamPolicyMetaStore.getIAMPolicyRecord(resourceType, resourceId);
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
            case IAM_POLICY -> throw new IllegalArgumentException("IAM Policy is not a resource");
        };
    }
}
