package com.flipkart.varadhi.entities;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
<<<<<<< HEAD
<<<<<<< HEAD
 * Per-region failover and produce policy for a {@link VaradhiTopic}.
=======
 * Per-region failover and replication policy for a {@link VaradhiTopic}.
>>>>>>> de7413df (added regionConfig in varadhiTopic)
=======
 * Per-region failover and produce policy for a {@link VaradhiTopic}.
>>>>>>> 2fb86390 (fixed classes for varadhiTopic region)
 */
@Data
@NoArgsConstructor
public class RegionConfig {

<<<<<<< HEAD
<<<<<<< HEAD
=======
    private boolean isReplicated = false;
>>>>>>> de7413df (added regionConfig in varadhiTopic)
=======
>>>>>>> 2fb86390 (fixed classes for varadhiTopic region)
    private boolean produceAllowed = true;
    /** Per-region standby; nullable. */
    private RegionName failOverRegion;

<<<<<<< HEAD
<<<<<<< HEAD
    public RegionConfig(boolean produceAllowed, RegionName failOverRegion) {
=======
    public RegionConfig(boolean isReplicated, boolean produceAllowed, RegionName failOverRegion) {
        this.isReplicated = isReplicated;
>>>>>>> de7413df (added regionConfig in varadhiTopic)
=======
    public RegionConfig(boolean produceAllowed, RegionName failOverRegion) {
>>>>>>> 2fb86390 (fixed classes for varadhiTopic region)
        this.produceAllowed = produceAllowed;
        this.failOverRegion = failOverRegion;
    }

<<<<<<< HEAD
<<<<<<< HEAD
    /** Default for a newly deployed region that accepts produce. */
    public static RegionConfig producing() {
        return new RegionConfig(true, null);
=======
    /** Default for a newly deployed replicated region that accepts produce. */
    public static RegionConfig replicatedProducing() {
        return new RegionConfig(true, true, null);
>>>>>>> de7413df (added regionConfig in varadhiTopic)
=======
    /** Default for a newly deployed region that accepts produce. */
    public static RegionConfig producing() {
        return new RegionConfig(true, null);
>>>>>>> 2fb86390 (fixed classes for varadhiTopic region)
    }
}
