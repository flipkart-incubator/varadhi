package com.flipkart.varadhi.entities.cluster.failover;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.ToString;

/**
 * One row in the per-stage ack ledger persisted on a {@link StageSnapshot}.
 * Captures the final ack from a single pod for one stage.
 */
@Getter
@ToString
public final class PodAckSnapshot {
    private final String hostname;
    private final boolean ok;
    private final long ackTime;
    private final String errorMsg;

    @JsonCreator
    public PodAckSnapshot(
        @JsonProperty ("hostname") String hostname,
        @JsonProperty ("ok") boolean ok,
        @JsonProperty ("ackTime") long ackTime,
        @JsonProperty ("errorMsg") String errorMsg
    ) {
        this.hostname = hostname;
        this.ok = ok;
        this.ackTime = ackTime;
        this.errorMsg = errorMsg;
    }
}
