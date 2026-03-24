package com.flipkart.varadhi.entities;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

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
 */
@Getter
@EqualsAndHashCode (callSuper = true)
@ValidateResource (message = "Invalid Region name. Check naming constraints.")
public class Region extends MetaStoreEntity implements Validatable {

    /**
     * The current availability status of the region.
     * This can change at runtime based on region health.
     */
    @Setter
    private RegionStatus status;

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
     * Type-safe view of the region identifier (same as {@link #getName()} as a string).
     */
    public RegionName getRegionName() {
        return new RegionName(getName());
    }

    /**
     * Checks if message stack operations are available in this status.
     *
     * @return {@code true} if message stack is available, {@code false} otherwise
     */
    public boolean isMessageStackAvailable() {
        return status.isMessageStackAvailable();
    }

    /**
     * Checks if produce operations are available in this region.
     *
     * @return {@code true} if produce is available, {@code false} otherwise
     */
    public boolean isProduceAvailable() {
        return status.isProduceAvailable();
    }

    /**
     * Checks if consume operations are available in this region.
     *
     * @return {@code true} if consume is available, {@code false} otherwise
     */
    public boolean isConsumeAvailable() {
        return status.isConsumeAvailable();
    }

    /**
     * Checks if the region is fully available.
     *
     * @return {@code true} if region is fully available, {@code false} otherwise
     */
    public boolean isAvailable() {
        return status.isAvailable();
    }
}
