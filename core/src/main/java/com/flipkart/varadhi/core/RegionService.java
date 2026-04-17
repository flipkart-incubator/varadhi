package com.flipkart.varadhi.core;

import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.entities.Region;
import com.flipkart.varadhi.entities.RegionStatus;
import com.flipkart.varadhi.entities.web.RegionCreateRequest;
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
     * Persists a new region. The request must contain only name and status; version is assigned by the store.
     */
    public Region createRegion(RegionCreateRequest request) {
        Region region = request.toRegion();
        regionStore.create(region);
        return region;
    }

    /**
     * Updates the status of an existing region (optimistic locking uses the version from the loaded entity).
     */
    public Region updateRegionStatus(String regionName, RegionStatus newStatus) {
        Region current = regionStore.get(regionName);
        if (current == null) {
            throw new ResourceNotFoundException(String.format("Region(%s) not found.", regionName));
        }
        Region updated = current.withStatus(newStatus);
        regionStore.update(updated);
        return updated;
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
