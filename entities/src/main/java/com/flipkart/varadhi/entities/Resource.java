package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.flipkart.varadhi.entities.auth.IamPolicyRecord;
import lombok.Getter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a versioned resource in the system.
 * <p>
 * This class extends {@link Versioned} to include a name and version, and is used to encapsulate
 * various types of entities such as topics, subscriptions, or composite objects like organization details.
 * <p>
 * It also provides a static factory method to create an {@link EntityResource} for a given entity.
 * <p>
 * The {@link EntityResource} is a specialized subclass of {@code Resource} that associates an entity
 * with its corresponding {@link ResourceType}.
 */

@JsonTypeInfo (use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "resourceType")
@JsonSubTypes ({
    @JsonSubTypes.Type (value = OrgDetails.class, name = "ORG"),
    @JsonSubTypes.Type (value = Resource.EntityResource.class, name = "PROJECT"),
    @JsonSubTypes.Type (value = Resource.EntityResource.class, name = "TEAM"),
    @JsonSubTypes.Type (value = Resource.EntityResource.class, name = "IAM_POLICY"),
    @JsonSubTypes.Type (value = Resource.EntityResource.class, name = "VARADHI_TOPIC"),
    @JsonSubTypes.Type (value = Resource.EntityResource.class, name = "VARADHI_SUBSCRIPTION")})
@Getter
public class Resource extends Versioned {
    @JsonProperty ("resourceType")
    private final ResourceType resourceType;

    public Resource(String name, int version, ResourceType resourceType) {
        super(name, version);
        this.resourceType = resourceType;
    }

    public static <T extends MetaStoreEntity> EntityResource<T> of(
        T entity,
        MetaStoreEntityType metaStoreEntityType,
        ResourceType resourceType
    ) {
        return new EntityResource<>(entity, metaStoreEntityType, resourceType);
    }

    @JsonTypeInfo (use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "metaStoreEntityType")
    @JsonSubTypes ({
        @JsonSubTypes.Type (value = Project.class, name = "PROJECT"),
        @JsonSubTypes.Type (value = Team.class, name = "TEAM"),
        @JsonSubTypes.Type (value = IamPolicyRecord.class, name = "IAM_POLICY"),
        @JsonSubTypes.Type (value = VaradhiTopic.class, name = "VARADHI_TOPIC"),
        @JsonSubTypes.Type (value = VaradhiSubscription.class, name = "VARADHI_SUBSCRIPTION"),})
    @Getter
    public static class EntityResource<T extends MetaStoreEntity> extends Resource {
        private final T entity;
        private final MetaStoreEntityType metaStoreEntityType;

        @JsonCreator
        public EntityResource(
            @JsonProperty ("entity") T entity,
            @JsonProperty ("metaStoreEntityType") MetaStoreEntityType metaStoreEntityType,
            ResourceType resourceType
        ) {
            super(entity.getName(), entity.getVersion(), resourceType);
            this.entity = entity;
            this.metaStoreEntityType = metaStoreEntityType;
        }
    }
}
