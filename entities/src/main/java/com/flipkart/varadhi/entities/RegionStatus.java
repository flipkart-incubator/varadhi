package com.flipkart.varadhi.entities;

/**
 * Represents the availability status of a region.
 * <p>
 * This status indicates whether a region is available for serving requests,
 * and if not, which specific operations are unavailable.
 */
public enum RegionStatus {
    /**
     * Region is fully available for both produce and consume operations.
     */
    AVAILABLE,

    /**
     * Region is completely unavailable for all operations.
     * This typically indicates a complete region failure or network isolation.
     */
    UNAVAILABLE,

    /**
     * Region is available for consume operations but unavailable for produce operations.
     * This may occur when the produce layer is down but consume layer is operational.
     */
    PRODUCE_UNAVAILABLE,

    /**
     * Region is available for produce operations but unavailable for consume operations.
     * This may occur when the consume layer is down but produce layer is operational.
     */
    CONSUME_UNAVAILABLE,
    /**
     * Region is available, Varadhi Components are also available but Messaging Stack is unavailable.
     * This may occur when message stack is unavailable in the region.
     */
    MSP_UNAVAILABLE;


    /**
     * Checks if message stack operations are available in this status.
     *
     * @return {@code true} if message stack is available, {@code false} otherwise
     */
    public boolean isMessageStackAvailable() {
        return this != MSP_UNAVAILABLE && this == AVAILABLE;
    }

    /**
     * Checks if produce operations are available in this status.
     *
     * @return {@code true} if produce is available, {@code false} otherwise
     */
    public boolean isProduceAvailable() {
        return this == AVAILABLE || this == CONSUME_UNAVAILABLE;
    }

    /**
     * Checks if consume operations are available in this status.
     *
     * @return {@code true} if consume is available, {@code false} otherwise
     */
    public boolean isConsumeAvailable() {
        return this == AVAILABLE || this == PRODUCE_UNAVAILABLE;
    }

    /**
     * Checks if the region is fully available.
     *
     * @return {@code true} if status is AVAILABLE, {@code false} otherwise
     */
    public boolean isAvailable() {
        return this == AVAILABLE;
    }
}
