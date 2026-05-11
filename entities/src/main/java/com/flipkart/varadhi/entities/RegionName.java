package com.flipkart.varadhi.entities;

import java.util.Objects;

/**
 * Represents a region name as a value object.
 * <p>
 * This class provides type safety for region names and ensures they are not null or blank.
 */
public record RegionName(String value) {

    /**
     * Special region name that is accepted during cluster bootstrapping when no regions have been registered in the
     * metastore yet. Once at least one region exists in the metastore every node must be configured with a valid,
     * registered region name.
     */
    public static final RegionName BOOTSTRAP_REGION = new RegionName("default");

    public RegionName {
        Objects.requireNonNull(value, "region name cannot be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException("region name cannot be blank");
        }
    }
}
