package com.flipkart.varadhi.core.cluster.failover;

import com.flipkart.varadhi.entities.RegionName;

/**
 * Cluster-bus request envelope for the controller's topic-failover APIs (web → controller).
 * For create, all fields are set; for get/abort only {@code topicFqn} (and {@code requestedBy} for
 * abort) are meaningful.
 *
 * <p>On the create path, {@code sourceRegion} is required (not inferred) for the same reason as
 * {@link com.flipkart.varadhi.entities.cluster.failover.TopicFailoverRequest}: it makes the failover
 * direction explicit and lets the controller validate it against the topic's current producing
 * region, preventing a wrong-way failover.
 */
public record FailoverApiRequest(
    String topicFqn,
    RegionName sourceRegion,
    RegionName targetRegion,
    boolean waitForReplicationLagToClear,
    String requestedBy
) {
    public static FailoverApiRequest of(String topicFqn) {
        return new FailoverApiRequest(topicFqn, null, null, false, null);
    }

    public static FailoverApiRequest of(String topicFqn, String requestedBy) {
        return new FailoverApiRequest(topicFqn, null, null, false, requestedBy);
    }
}
