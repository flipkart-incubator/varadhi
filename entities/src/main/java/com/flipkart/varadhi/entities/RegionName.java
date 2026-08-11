package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonCreator.Mode;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Objects;

/**
 * Represents a region name as a value object.
 * <p>
 * This class provides type safety for region names and ensures they are not null or blank. It is
 * used as the typed key for per-region {@code produceConfigs} on {@link VaradhiTopic} and for the
 * source/target region fields on the topic-failover APIs, instead of a raw {@link String}, so those
 * call sites cannot accidentally mix region names with other string identifiers.
 */
public record RegionName(@JsonValue String value) {

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

    /**
     * Factory for a {@link RegionName} from its string value.
     *
     * @param value the region name string; must be non-null and non-blank
     * @return a validated {@link RegionName}
     */
    @JsonCreator (mode = Mode.DELEGATING)
    public static RegionName of(String value) {
        return new RegionName(value);
    }
}
