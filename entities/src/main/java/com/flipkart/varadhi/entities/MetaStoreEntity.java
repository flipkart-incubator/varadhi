package com.flipkart.varadhi.entities;

import lombok.Getter;

@Getter
public class MetaStoreEntity extends VersionedEntity {
    public static final String NAME_SEPARATOR = ".";
    public static final String NAME_SEPARATOR_REGEX = "\\.";

    protected MetaStoreEntity(String name, int version) {
        super(name, version);
    }
}
