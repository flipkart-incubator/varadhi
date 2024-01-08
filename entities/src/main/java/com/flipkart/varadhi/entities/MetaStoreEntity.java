package com.flipkart.varadhi.entities;

import lombok.Getter;

@Getter
public class MetaStoreEntity extends VersionedEntity {
    public static final String NAME_SEPARATOR = ".";

    protected MetaStoreEntity(String name, int version) {
        super(name, version);
    }
}
