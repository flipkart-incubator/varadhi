package com.flipkart.varadhi.core;

import com.flipkart.varadhi.entities.Region;
import com.flipkart.varadhi.spi.db.RegionStore;

import java.util.List;
import java.util.Objects;

/**
 * Service layer for {@link Region} metadata.
 * <p>
 * Delegates to {@link RegionStore}; use this class from handlers and other services instead of
 * accessing the store directly.
 */
public class RegionService {

    private final RegionStore regionStore;

    public RegionService(RegionStore regionStore) {
        this.regionStore = Objects.requireNonNull(regionStore, "RegionStore cannot be null");
    }

    /**
     * Persists a new region.
     *
     * @param region the region to create
     * @return the same region instance after persistence
     */
    public Region createRegion(Region region) {
        regionStore.create(region);
        return region;
    }

    /**
     * Loads a region by id.
     *
     * @param regionName metastore region name (see {@link Region#getName()})
     * @return the region
     */
    public Region getRegion(String regionName) {
        return regionStore.get(regionName);
    }

    /**
     * Returns all regions in the metastore.
     */
    public List<Region> getRegions() {
        return regionStore.getAll();
    }

    /**
     * @param regionName metastore region name
     * @return {@code true} if the region exists
     */
    public boolean regionExists(String regionName) {
        return regionStore.exists(regionName);
    }

    /**
     * Deletes a region from the metastore.
     *
     * @param regionName metastore region name
     */
    public void deleteRegion(String regionName) {
        regionStore.delete(regionName);
    }
}
