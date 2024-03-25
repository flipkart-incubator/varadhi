package com.flipkart.varadhi.services;

import com.flipkart.varadhi.entities.auth.IamPolicyRecord;
import com.flipkart.varadhi.entities.auth.IamPolicyRequest;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.spi.db.IamPolicyMetaStore;
import com.flipkart.varadhi.spi.db.MetaStore;

import java.util.HashMap;

import static com.flipkart.varadhi.entities.VersionedEntity.NAME_SEPARATOR;
import static com.flipkart.varadhi.utils.IamPolicyHelper.getAuthResourceFQN;

public class IamPolicyService {
    private final MetaStore metaStore;
    private final IamPolicyMetaStore iamPolicyMetaStore;

    public IamPolicyService(MetaStore metaStore, IamPolicyMetaStore iamPolicyMetaStore) {
        this.metaStore = metaStore;
        this.iamPolicyMetaStore = iamPolicyMetaStore;
    }

    private IamPolicyRecord createIamPolicyRecord(String resourceId, ResourceType resourceType) {
        if (!isResourceValid(resourceId, resourceType)) {
            throw new IllegalArgumentException(
                    "Invalid resource id(%s) for resource type(%s).".formatted(resourceId, resourceType));
        }
        IamPolicyRecord policyRecord =
                new IamPolicyRecord(getAuthResourceFQN(resourceType, resourceId), 0, new HashMap<>());
        iamPolicyMetaStore.createIamPolicyRecord(policyRecord);
        return policyRecord;
    }

    public IamPolicyRecord getIamPolicy(ResourceType resourceType, String resourceId) {
        return iamPolicyMetaStore.getIamPolicyRecord(getAuthResourceFQN(resourceType, resourceId));
    }

    public IamPolicyRecord setIamPolicy(ResourceType resourceType, String resourceId, IamPolicyRequest binding) {
        IamPolicyRecord policyRecord = createOrGetIamPolicyRecord(resourceId, resourceType);
        policyRecord.setRoleAssignment(binding.getSubject(), binding.getRoles());
        return updateIamPolicyRecord(policyRecord);
    }

    public void deleteIamPolicy(ResourceType resourceType, String resourceId) {
        boolean exists = iamPolicyMetaStore.isIamPolicyRecordPresent(getAuthResourceFQN(resourceType, resourceId));
        if (!exists) {
            throw new ResourceNotFoundException(String.format(
                    "Iam Policy Record on resource(%s) not found.",
                    resourceId
            ));
        }
        iamPolicyMetaStore.deleteIamPolicyRecord(getAuthResourceFQN(resourceType, resourceId));
    }

    private IamPolicyRecord createOrGetIamPolicyRecord(String resourceId, ResourceType resourceType) {
        boolean exists = iamPolicyMetaStore.isIamPolicyRecordPresent(getAuthResourceFQN(resourceType, resourceId));
        if (!exists) {
            return createIamPolicyRecord(resourceId, resourceType);
        }
        return iamPolicyMetaStore.getIamPolicyRecord(getAuthResourceFQN(resourceType, resourceId));
    }

    private IamPolicyRecord updateIamPolicyRecord(IamPolicyRecord iamPolicyRecord) {
        IamPolicyRecord existingNode =
                iamPolicyMetaStore.getIamPolicyRecord(iamPolicyRecord.getName());
        if (iamPolicyRecord.getVersion() != existingNode.getVersion()) {
            throw new InvalidOperationForResourceException(String.format(
                    "Conflicting update, IamPolicyRecord(%s) has been modified. Fetch latest and try again.",
                    iamPolicyRecord.getName()
            ));
        }
        int updatedVersion = iamPolicyMetaStore.updateIamPolicyRecord(iamPolicyRecord);
        iamPolicyRecord.setVersion(updatedVersion);
        return iamPolicyRecord;
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
            case IAM_POLICY -> throw new IllegalArgumentException("Iam Policy is not a resource");
        };
    }
}
