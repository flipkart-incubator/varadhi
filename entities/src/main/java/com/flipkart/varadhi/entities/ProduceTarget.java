package com.flipkart.varadhi.entities;

/**
 * Resolved produce target for a request region: the storage topic segment to write and the region
 * key used for the producer cache (local region, or {@link ProduceConfig#failOverRegion()} when set).
 */
public record ProduceTarget(StorageTopic storageTopic, RegionName produceRegion) {
}
