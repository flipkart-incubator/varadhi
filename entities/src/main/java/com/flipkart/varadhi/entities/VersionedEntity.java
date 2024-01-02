package com.flipkart.varadhi.entities;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.Setter;

@Getter
public abstract class VersionedEntity implements Validatable {
    public static final int INITIAL_VERSION = 0;

    @NotBlank
    private final String name;

    @Setter
    @Min(0)
    private int version;

    protected VersionedEntity(String name, int version) {
        this.name = name;
        this.version = version;
    }

    protected VersionedEntity(String name) {
        this(name, INITIAL_VERSION);
    }
}
