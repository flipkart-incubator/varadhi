package com.flipkart.varadhi.core.cluster.failover;

/**
 * Cluster-bus request envelope for the controller's topic-failover APIs (web → controller).
 * For create, all fields are set; for get/abort only {@code topicFqn} (and {@code requestedBy} for
 * abort) are meaningful.
 */
public record FailoverApiRequest(
    String topicFqn,
    String sourceRegion,
    String targetRegion,
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
