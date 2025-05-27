package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.entities.auth.EntityType;
import lombok.Getter;

// check for versiondedEntity usecase if any while coding
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

        public EntityType getResourceType() {
            return entityType;
        }
    }

}
