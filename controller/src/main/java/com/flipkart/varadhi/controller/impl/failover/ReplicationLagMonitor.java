package com.flipkart.varadhi.controller.impl.failover;

import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.VaradhiTopic;

import java.util.OptionalLong;

/**
 * Reads source-region replication lag for a topic during DRAIN. Empty means the messaging stack
 * does not expose lag (caller should fail or skip based on policy).
 */
@FunctionalInterface
public interface ReplicationLagMonitor {

    /**
     * @return lag in messages ({@code 0} = clear), or empty if unsupported
     */
    OptionalLong replicationLag(VaradhiTopic topic, RegionName sourceRegion);

    /** Always-unsupported monitor — used until StorageTopicService gains a lag API. */
    static ReplicationLagMonitor unsupported() {
        return (topic, sourceRegion) -> OptionalLong.empty();
    }
}
