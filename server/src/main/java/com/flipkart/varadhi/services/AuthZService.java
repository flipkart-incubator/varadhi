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
import static com.flipkart.varadhi.utils.AuthZHelper.getAuthResourceFQN;

public class AuthZService {
    private final MetaStore metaStore;
    private final IAMPolicyMetaStore iamPolicyMetaStore;

    public AuthZService(MetaStore metaStore, IAMPolicyMetaStore iamPolicyMetaStore) {
        this.metaStore = metaStore;
        this.iamPolicyMetaStore = iamPolicyMetaStore;
    }

    public List<IAMPolicyRecord> getAll() {
        return iamPolicyMetaStore.getIAMPolicyRecords();
    }

    private IAMPolicyRecord createIAMPolicyRecord(String resourceId, ResourceType resourceType) {
        if (!isResourceValid(resourceId, resourceType)) {
            throw new IllegalArgumentException(
                    "Invalid resource id(%s) for resource type(%s).".formatted(resourceId, resourceType));
        }
        IAMPolicyRecord node = new IAMPolicyRecord(getAuthResourceFQN(resourceType, resourceId), new HashMap<>(), 0);
        iamPolicyMetaStore.createIAMPolicyRecord(node);
        return node;
    }

    public IAMPolicyRecord getIAMPolicy(ResourceType resourceType, String resourceId) {
        return iamPolicyMetaStore.getIAMPolicyRecord(getAuthResourceFQN(resourceType, resourceId));
    }

    public IAMPolicyRecord setIAMPolicy(ResourceType resourceType, String resourceId, IAMPolicyRequest binding) {
        IAMPolicyRecord node = createOrGetIAMPolicyRecord(resourceId, resourceType);
        node.setRoleAssignment(binding.getSubject(), binding.getRoles());
        return updateIAMPolicyRecord(node);
    }

    public void deleteIAMPolicy(ResourceType resourceType, String resourceId) {
        boolean exists = iamPolicyMetaStore.isIAMPolicyRecordPresent(getAuthResourceFQN(resourceType, resourceId));
        if (!exists) {
            throw new ResourceNotFoundException(String.format(
                    "IAM Policy Record on resource(%s) not found.",
                    resourceId
            ));
        }
        iamPolicyMetaStore.deleteIAMPolicyRecord(getAuthResourceFQN(resourceType, resourceId));
    }

    private IAMPolicyRecord createOrGetIAMPolicyRecord(String resourceId, ResourceType resourceType) {
        boolean exists = iamPolicyMetaStore.isIAMPolicyRecordPresent(getAuthResourceFQN(resourceType, resourceId));
        if (!exists) {
            return createIAMPolicyRecord(resourceId, resourceType);
        }
        return iamPolicyMetaStore.getIAMPolicyRecord(getAuthResourceFQN(resourceType, resourceId));
    }

    private IAMPolicyRecord updateIAMPolicyRecord(IAMPolicyRecord node) {
        IAMPolicyRecord existingNode =
                iamPolicyMetaStore.getIAMPolicyRecord(node.getAuthResourceId());
        if (node.getVersion() != existingNode.getVersion()) {
            throw new InvalidOperationForResourceException(String.format(
                    "Conflicting update, IAMPolicyRecord(%s) has been modified. Fetch latest and try again.",
                    node.getAuthResourceId()
            ));
        }
        int updatedVersion = iamPolicyMetaStore.updateIAMPolicyRecord(node);
        node.setVersion(updatedVersion);
        return node;
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
            case SUBSCRIPTION -> {
                // project:subscription
                String[] segments = resourceId.split(":");
                String subscriptionName = String.join(NAME_SEPARATOR, segments[0], segments[1]);
                yield (segments.length == 2) && metaStore.checkSubscriptionExists(subscriptionName);
            }
            case IAM_POLICY -> throw new IllegalArgumentException("IAM Policy is not a resource");
        };
    }
}
