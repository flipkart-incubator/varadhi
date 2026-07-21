package com.flipkart.varadhi.entities;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Per-region failover and produce policy for a {@link VaradhiTopic}.
 */
@Data
@NoArgsConstructor
public class RegionConfig {

    private boolean produceAllowed = true;
    /** Per-region standby; nullable. */
    private RegionName failOverRegion;

    public RegionConfig(boolean produceAllowed, RegionName failOverRegion) {
        this.produceAllowed = produceAllowed;
        this.failOverRegion = failOverRegion;
    }

    /** Default for a newly deployed region that accepts produce. */
    public static RegionConfig producing() {
        return new RegionConfig(true, null);
    }
}
