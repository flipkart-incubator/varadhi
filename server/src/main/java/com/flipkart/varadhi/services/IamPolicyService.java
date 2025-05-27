package com.flipkart.varadhi.services;

import com.flipkart.varadhi.entities.auth.EntityType;
import com.flipkart.varadhi.entities.auth.IamPolicyRecord;
import com.flipkart.varadhi.entities.auth.IamPolicyRequest;
import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.spi.db.IamPolicyStore;
import com.flipkart.varadhi.spi.db.MetaStore;

import java.util.HashMap;

import static com.flipkart.varadhi.entities.Versioned.NAME_SEPARATOR;
import static com.flipkart.varadhi.utils.IamPolicyHelper.getAuthResourceFQN;

public class IamPolicyService {
    private final MetaStore metaStore;
    private final IamPolicyStore iamPolicyStore;

    public IamPolicyService(MetaStore metaStore, IamPolicyStore iamPolicyStore) {
        this.metaStore = metaStore;
        this.iamPolicyStore = iamPolicyStore;
    }

    private IamPolicyRecord createIamPolicyRecord(String resourceId, EntityType entityType) {
        if (!isResourceValid(resourceId, entityType)) {
            throw new IllegalArgumentException(
                "Invalid resource id(%s) for resource type(%s).".formatted(resourceId, entityType)
            );
        }
        IamPolicyRecord policyRecord = new IamPolicyRecord(
            getAuthResourceFQN(entityType, resourceId),
            0,
            new HashMap<>()
        );
        iamPolicyStore.create(policyRecord);
        return policyRecord;
    }

    public IamPolicyRecord getIamPolicy(EntityType entityType, String resourceId) {
        return iamPolicyStore.get(getAuthResourceFQN(entityType, resourceId));
    }

    public IamPolicyRecord setIamPolicy(EntityType entityType, String resourceId, IamPolicyRequest binding) {
        IamPolicyRecord policyRecord = createOrGetIamPolicyRecord(resourceId, entityType);
        policyRecord.setRoleAssignment(binding.getSubject(), binding.getRoles());
        return updateIamPolicyRecord(policyRecord);
    }

    public void deleteIamPolicy(EntityType entityType, String resourceId) {
        iamPolicyStore.delete(getAuthResourceFQN(entityType, resourceId));
    }

    private IamPolicyRecord createOrGetIamPolicyRecord(String resourceId, EntityType entityType) {
        boolean exists = iamPolicyStore.exists(getAuthResourceFQN(entityType, resourceId));
        if (!exists) {
            return createIamPolicyRecord(resourceId, entityType);
        }
        return iamPolicyStore.get(getAuthResourceFQN(entityType, resourceId));
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

    private boolean isResourceValid(String resourceId, EntityType entityType) {
        return switch (entityType) {
            case ROOT -> throw new IllegalArgumentException(
                "ROOT is implicit resource type. No Iam policies supported on it."
            );
            case ORG, ORG_FILTER -> metaStore.orgs().exists(resourceId);
            case TEAM -> {
                // org:team
                String[] segments = resourceId.split(":");
                yield (segments.length == 2) && metaStore.teams().exists(segments[1], segments[0]);
            }
            case PROJECT -> metaStore.projects().exists(resourceId);
            case TOPIC -> {
                // project:topic
                String[] segments = resourceId.split(":");
                String varadhiTopicName = String.join(NAME_SEPARATOR, segments[0], segments[1]);
                yield (segments.length == 2) && metaStore.topics().exists(varadhiTopicName);
            }
            case SUBSCRIPTION -> {
                // project:subscription
                String[] segments = resourceId.split(":");
                String subscriptionName = String.join(NAME_SEPARATOR, segments[0], segments[1]);
                yield (segments.length == 2) && metaStore.subscriptions().exists(subscriptionName);
            }
            case IAM_POLICY -> throw new IllegalArgumentException("IamPolicy is not a policy owning resource.");
        };
    }
}
