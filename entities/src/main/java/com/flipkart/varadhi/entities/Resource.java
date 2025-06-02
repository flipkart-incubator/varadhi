package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.*;
import com.flipkart.varadhi.entities.auth.IamPolicyRecord;
import lombok.Getter;

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

@JsonTypeInfo (use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "resourceType")
@JsonSubTypes ({
    @JsonSubTypes.Type (value = OrgDetails.class, name = "ORG"),
    @JsonSubTypes.Type (value = Resource.EntityResource.class, name = "PROJECT"),
    @JsonSubTypes.Type (value = Resource.EntityResource.class, name = "TEAM"),
    @JsonSubTypes.Type (value = Resource.EntityResource.class, name = "IAM_POLICY"),
    @JsonSubTypes.Type (value = Resource.EntityResource.class, name = "TOPIC"),
    @JsonSubTypes.Type (value = Resource.EntityResource.class, name = "SUBSCRIPTION")})
@Getter
public class Resource extends Versioned {
    @JsonProperty ("resourceType")
    private final ResourceType resourceType;

    public Resource(String name, int version, ResourceType resourceType) {
        super(name, version);
        this.resourceType = resourceType;
    }

    public static <T extends MetaStoreEntity> EntityResource<T> of(T entity, ResourceType resourceType) {
        return new EntityResource<>(entity, resourceType);
    }

    @Getter
    public static class EntityResource<T extends MetaStoreEntity> extends Resource {

        @JsonTypeInfo (use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "entityType")
        @JsonSubTypes ({
            @JsonSubTypes.Type (value = Project.class, name = "PROJECT"),
            @JsonSubTypes.Type (value = Team.class, name = "TEAM"),
            @JsonSubTypes.Type (value = IamPolicyRecord.class, name = "IAM_POLICY"),
            @JsonSubTypes.Type (value = VaradhiTopic.class, name = "TOPIC"),
            @JsonSubTypes.Type (value = VaradhiSubscription.class, name = "SUBSCRIPTION"),})
        private final T entity;

        @JsonCreator
        public EntityResource(
            @JsonProperty ("entity") T entity,
            @JsonProperty ("resourceType") ResourceType resourceType
        ) {
            super(entity.getName(), entity.getVersion(), resourceType);
            this.entity = entity;
        }
    }
}
