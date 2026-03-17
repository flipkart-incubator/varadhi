package com.flipkart.varadhi.entities;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

/**
 * Base class for all entities in Varadhi. Such entities are versioned and named.
 */
@Getter
@Setter
@EqualsAndHashCode (exclude = "version")
public abstract class Versioned {
    public static final int INITIAL_VERSION = 0;
    public static final String NAME_SEPARATOR = ".";
    public static final String NAME_SEPARATOR_REGEX = "\\.";

    private String name;
    private int version;

    protected Versioned(String name, int version) {
        this.name = name;
        this.version = version;
    }

    protected Versioned(String name) {
        this(name, INITIAL_VERSION);
    }

    /** No-arg constructor for subclasses that set name/version after construction (e.g. JSON deserialization). */
    protected Versioned() {
        this.name = null;
        this.version = INITIAL_VERSION;
    }
}
