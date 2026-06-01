package com.flipkart.varadhi.entities.cluster.failover;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.ToString;

/**
 * REST request body for {@code POST /v1/projects/:project/topics/:topic/failover}.
 */
@Getter
@ToString
public final class TopicFailoverRequest {
    private final String toRegion;
    private final boolean waitForReplicationLagToClear;
    private final boolean skipValidation;

    @JsonCreator
    public TopicFailoverRequest(
        @JsonProperty ("toRegion") String toRegion,
        @JsonProperty ("waitForReplicationLagToClear") boolean waitForReplicationLagToClear,
        @JsonProperty ("skipValidation") boolean skipValidation
    ) {
        this.toRegion = toRegion;
        this.waitForReplicationLagToClear = waitForReplicationLagToClear;
        this.skipValidation = skipValidation;
    }
}
