package com.flipkart.varadhi.entities;

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
public class Resource extends Versioned {
    protected Resource(String name, int version) {
        super(name, version);
    }

    public static <T extends MetaStoreEntity> EntityResource<T> of(T entity, EntityType entityType) {
        return new EntityResource<>(entity, entityType);
    }

    public static class EntityResource<T extends MetaStoreEntity> extends Resource {
        @Getter
        private final T entity;
        private final EntityType entityType;

        private EntityResource(T entity, EntityType entityType) {
            super(entity.getName(), entity.getVersion());
            this.entity = entity;
            this.entityType = entityType;
        }

        public EntityType getEntityType() {
            return entityType;
        }
    }

}
