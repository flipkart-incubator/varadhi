package com.flipkart.varadhi.entities;

import java.util.Objects;

/**
 * Represents a region name as a value object.
 * <p>
 * This class provides type safety for region names and ensures they are not null or blank.
 */
public record RegionName(String value) {

    public RegionName {
        Objects.requireNonNull(value, "region name cannot be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException("region name cannot be blank");
        }
    }
}
