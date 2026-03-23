package com.flipkart.varadhi.spi.db;

import com.flipkart.varadhi.entities.Region;

import java.util.List;

/**
 * Persistence abstraction for {@link Region} entities (logical regions / zones).
 * <p>
 * Implementations (e.g. ZooKeeper) store each region under a dedicated path keyed by
 * {@link com.flipkart.varadhi.entities.MetaStoreEntity#getName() the region id} (same string as
 * {@link com.flipkart.varadhi.entities.RegionName RegionName}). Use {@link MetaStore#regions()}
 * to obtain the store from the configured metastore.
 * <p>
 * <b>Concurrency:</b> Implementations should be safe for use from the Varadhi service layer; callers
 * are responsible for validation (e.g. duplicate create, delete of non-existent region).
 */
public interface RegionStore {

    /**
     * Persists a new region. The region name must be unique.
     *
     * @param region the region to create (must not be null)
     * @throws com.flipkart.varadhi.spi.db.MetaStoreException if creation fails or the region already exists
     */
    void create(Region region);

    /**
     * Loads a region by its name (the string value of {@link com.flipkart.varadhi.entities.RegionName}).
     *
     * @param regionName region identifier (same as {@link com.flipkart.varadhi.entities.MetaStoreEntity#getName()})
     * @return the region, or throws if not found per implementation
     */
    Region get(String regionName);

    /**
     * Returns all regions known to the metastore.
     */
    List<Region> getAll();

    /**
     * @param regionName region identifier
     * @return {@code true} if a region with this name exists
     */
    boolean exists(String regionName);

    /**
     * Removes the region from the metastore.
     *
     * @param regionName region identifier
     * @throws com.flipkart.varadhi.common.exceptions.ResourceNotFoundException if the region does not exist (typical)
     */
    void delete(String regionName);
}
