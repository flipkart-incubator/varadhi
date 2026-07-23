package com.flipkart.varadhi.entities;

/**
 * Per-region failover and produce policy for a {@link VaradhiTopic}.
 */
public record RegionConfig(boolean produceAllowed, RegionName failOverRegion) {

    /** Default for a newly deployed region that accepts produce. */
    public static RegionConfig producing() {
        return new RegionConfig(true, null);
    }
}
