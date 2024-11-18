package com.flipkart.varadhi.entities.cluster;

public enum ShardAssignmentState {
    /**
     * Shard has been created & is well formed but not assigned yet
     * ?? how to deal with create failed and delete failed ?? -- may be FAILED
     */
    STOPPED,
    /**
     * Shard is assigned to a subscription and is in the process of starting
     */
    STARTING,

    /**
     * Shard is running
     */
    STARTED,

    /**
     * Shard is stopping, still assigned though
     */
    STOPPING
}
