package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Represents a region in Varadhi.
 * <p>
 * A region is a logical grouping of resources (topics, subscriptions) that are
 * typically deployed in the same data center or availability zone.
 * <p>
 * The region has a status that indicates its availability for serving requests.
 * Status can change at runtime based on region health and failures.
 * <p>
 * Extends {@link MetaStoreEntity} so it can be persisted via the metastore (e.g. ZooKeeper).
 * <p>
 * Unknown JSON properties are ignored so clients may send extra fields without failing deserialization
 * (prefer {@link com.flipkart.varadhi.entities.web.RegionCreateRequest} for create APIs).
 */
@Getter
@EqualsAndHashCode (callSuper = true)
@JsonIgnoreProperties (ignoreUnknown = true)
@ValidateResource (message = "Invalid Region name. Check naming constraints.")
public class Region extends MetaStoreEntity implements Validatable {

    /**
     * The current availability status of the region. Immutable on an instance; use {@link #withStatus(RegionStatus)}
     * to produce a copy with a different status (e.g. during {@code PATCH /v1/regions/:region}).
     */
    private final RegionStatus status;

    public Region(String name, int version, RegionStatus status) {
        super(name, version, MetaStoreEntityType.REGION);
        this.status = status;
    }

    /**
     * Creates a region with initial version.
     */
    public static Region of(RegionName regionName, RegionStatus status) {
        return new Region(regionName.value(), INITIAL_VERSION, status);
    }

    /**
     * Returns a new {@code Region} with the given status; preserves name and version. Mirrors Lombok's {@code @With}
     * semantics (kept manual because {@code @With} cannot see fields inherited from {@link MetaStoreEntity}).
     * <p>
     * If the new status is identical to the current one the same instance is returned (no-op copy).
     */
    public Region withStatus(RegionStatus newStatus) {
        return this.status == newStatus ? this : new Region(getName(), getVersion(), newStatus);
    }

    /**
     * Type-safe view of the region identifier (same as {@link #getName()} as a string).
     */
    @JsonIgnore
    public RegionName getRegionName() {
        return new RegionName(getName());
    }

    /**
     * Checks if message stack operations are available in this status.
     *
     * @return {@code true} if message stack is available, {@code false} otherwise
     */
    @JsonIgnore
    public boolean isMessageStackAvailable() {
        return status.isMessageStackAvailable();
    }

    /**
     * Checks if produce operations are available in this region.
     *
     * @return {@code true} if produce is available, {@code false} otherwise
     */
    @JsonIgnore
    public boolean isProduceAvailable() {
        return status.isProduceAvailable();
    }

    /**
     * Checks if consume operations are available in this region.
     *
     * @return {@code true} if consume is available, {@code false} otherwise
     */
    @JsonIgnore
    public boolean isConsumeAvailable() {
        return status.isConsumeAvailable();
    }

    /**
     * Checks if the region is fully available.
     *
     * @return {@code true} if region is fully available, {@code false} otherwise
     */
    @JsonIgnore
    public boolean isAvailable() {
        return status.isAvailable();
    }
}
