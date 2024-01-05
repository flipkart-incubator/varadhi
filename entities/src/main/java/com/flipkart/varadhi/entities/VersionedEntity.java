package com.flipkart.varadhi.entities;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@EqualsAndHashCode
public abstract class VersionedEntity {
    public static final int INITIAL_VERSION = 0;

    private final String name;

    @Setter
    private int version;

    protected VersionedEntity(String name, int version) {
        this.name = name;
        this.version = version;
    }

    protected VersionedEntity(String name) {
        this(name, INITIAL_VERSION);
    }
}
