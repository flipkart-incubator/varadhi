package com.flipkart.varadhi.entities;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

/**
 * Base class for all entities in Varadhi. Such entities are versioned and named.
 */
@Getter
@EqualsAndHashCode (exclude = "version")
public abstract class Versioned {
    public static final int INITIAL_VERSION = 0;
    public static final String NAME_SEPARATOR = ".";
    public static final String NAME_SEPARATOR_REGEX = "\\.";

    private final String name;

    @Setter
    private int version;

    protected Versioned(String name, int version) {
        this.name = name;
        this.version = version;
    }

    protected Versioned(String name) {
        this(name, INITIAL_VERSION);
    }
}
