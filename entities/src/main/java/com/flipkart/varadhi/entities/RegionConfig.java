package com.flipkart.varadhi.entities;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
<<<<<<< HEAD
 * Per-region failover and produce policy for a {@link VaradhiTopic}.
=======
 * Per-region failover and replication policy for a {@link VaradhiTopic}.
>>>>>>> de7413df (added regionConfig in varadhiTopic)
 */
@Data
@NoArgsConstructor
public class RegionConfig {

<<<<<<< HEAD
=======
    private boolean isReplicated = false;
>>>>>>> de7413df (added regionConfig in varadhiTopic)
    private boolean produceAllowed = true;
    /** Per-region standby; nullable. */
    private RegionName failOverRegion;

<<<<<<< HEAD
    public RegionConfig(boolean produceAllowed, RegionName failOverRegion) {
=======
    public RegionConfig(boolean isReplicated, boolean produceAllowed, RegionName failOverRegion) {
        this.isReplicated = isReplicated;
>>>>>>> de7413df (added regionConfig in varadhiTopic)
        this.produceAllowed = produceAllowed;
        this.failOverRegion = failOverRegion;
    }

<<<<<<< HEAD
    /** Default for a newly deployed region that accepts produce. */
    public static RegionConfig producing() {
        return new RegionConfig(true, null);
=======
    /** Default for a newly deployed replicated region that accepts produce. */
    public static RegionConfig replicatedProducing() {
        return new RegionConfig(true, true, null);
>>>>>>> de7413df (added regionConfig in varadhiTopic)
    }
}
