package com.flipkart.varadhi.services;

import com.flipkart.varadhi.entities.auth.IamPolicyRecord;
import com.flipkart.varadhi.entities.auth.IamPolicyRequest;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.spi.db.IamPolicyStore;
import com.flipkart.varadhi.spi.db.MetaStore;

import java.util.HashMap;

import static com.flipkart.varadhi.entities.VersionedEntity.NAME_SEPARATOR;
import static com.flipkart.varadhi.utils.IamPolicyHelper.getAuthResourceFQN;

public class IamPolicyService {
    private final MetaStore metaStore;
    private final IamPolicyStore iamPolicyStore;

    public IamPolicyService(MetaStore metaStore, IamPolicyStore iamPolicyStore) {
        this.metaStore = metaStore;
        this.iamPolicyStore = iamPolicyStore;
    }

    private IamPolicyRecord createIamPolicyRecord(String resourceId, ResourceType resourceType) {
        if (!isResourceValid(resourceId, resourceType)) {
            throw new IllegalArgumentException(
                "Invalid resource id(%s) for resource type(%s).".formatted(resourceId, resourceType)
            );
        }
        IamPolicyRecord policyRecord = new IamPolicyRecord(
            getAuthResourceFQN(resourceType, resourceId),
            0,
            new HashMap<>()
        );
        iamPolicyStore.create(policyRecord);
        return policyRecord;
    }

    public IamPolicyRecord getIamPolicy(ResourceType resourceType, String resourceId) {
        return iamPolicyStore.get(getAuthResourceFQN(resourceType, resourceId));
    }

    public IamPolicyRecord setIamPolicy(ResourceType resourceType, String resourceId, IamPolicyRequest binding) {
        IamPolicyRecord policyRecord = createOrGetIamPolicyRecord(resourceId, resourceType);
        policyRecord.setRoleAssignment(binding.getSubject(), binding.getRoles());
        return updateIamPolicyRecord(policyRecord);
    }

    public void deleteIamPolicy(ResourceType resourceType, String resourceId) {
        iamPolicyStore.delete(getAuthResourceFQN(resourceType, resourceId));
    }

    private IamPolicyRecord createOrGetIamPolicyRecord(String resourceId, ResourceType resourceType) {
        boolean exists = iamPolicyStore.exists(getAuthResourceFQN(resourceType, resourceId));
        if (!exists) {
            return createIamPolicyRecord(resourceId, resourceType);
        }
        return iamPolicyStore.get(getAuthResourceFQN(resourceType, resourceId));
    }

    private IamPolicyRecord updateIamPolicyRecord(IamPolicyRecord iamPolicyRecord) {
        IamPolicyRecord existingNode = iamPolicyStore.get(iamPolicyRecord.getName());
        if (iamPolicyRecord.getVersion() != existingNode.getVersion()) {
            throw new InvalidOperationForResourceException(
                String.format(
                    "Conflicting update, IamPolicyRecord(%s) has been modified. Fetch latest and try again.",
                    iamPolicyRecord.getName()
                )
            );
        }
        iamPolicyStore.update(iamPolicyRecord);
        return iamPolicyRecord;
    }

    private boolean isResourceValid(String resourceId, ResourceType resourceType) {
        return switch (resourceType) {
            case ROOT -> throw new IllegalArgumentException(
                "ROOT is implicit resource type. No Iam policies supported on it."
            );
            case ORG -> metaStore.orgMetaStore().exists(resourceId);
            case TEAM -> {
                // org:team
                String[] segments = resourceId.split(":");
                yield (segments.length == 2) && metaStore.teamMetaStore().exists(segments[1], segments[0]);
            }
            case PROJECT -> metaStore.projectMetaStore().exists(resourceId);
            case TOPIC -> {
                // project:topic
                String[] segments = resourceId.split(":");
                String varadhiTopicName = String.join(NAME_SEPARATOR, segments[0], segments[1]);
                yield (segments.length == 2) && metaStore.topicMetaStore().exists(varadhiTopicName);
            }
            case SUBSCRIPTION -> {
                // project:subscription
                String[] segments = resourceId.split(":");
                String subscriptionName = String.join(NAME_SEPARATOR, segments[0], segments[1]);
                yield (segments.length == 2) && metaStore.subscriptionMetaStore().exists(subscriptionName);
            }
            case IAM_POLICY -> throw new IllegalArgumentException("IamPolicy is not a policy owning resource.");
        };
    }
}
