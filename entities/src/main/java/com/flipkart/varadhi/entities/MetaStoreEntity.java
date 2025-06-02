package com.flipkart.varadhi.entities;

import lombok.Getter;

@Getter
public class MetaStoreEntity extends Versioned {
    private final MetaStoreEntityType entityType;

    protected MetaStoreEntity(String name, int version, MetaStoreEntityType entityType) {
        super(name, version);
        this.entityType = entityType;
    }
}
