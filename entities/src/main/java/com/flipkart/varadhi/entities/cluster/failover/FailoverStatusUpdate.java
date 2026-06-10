package com.flipkart.varadhi.entities.cluster.failover;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.ToString;

import java.util.Objects;

/**
 * Pod -> Controller targeted ack (point-to-point via
 * {@link com.flipkart.varadhi.core.cluster.MessageExchange#send}) reporting the
 * outcome of one stage trigger.
 *
 * <p>The controller correlates the update with an in-flight stage barrier
 * ({@code StageAwaiter}) on {@code (opId, stage, fenceVersion, hostname)}.
 */
@Getter
@ToString
public final class FailoverStatusUpdate {

    private final String opId;
    private final String hostname;
    private final FailoverStage stage;
    private final long fenceVersion;

    /** {@code true} if the pod successfully observed / executed the requested stage action. */
    private final boolean ok;

    /** Non-null only when {@code ok == false}. */
    private final String errorMsg;

    @JsonCreator
    public FailoverStatusUpdate(
        @JsonProperty ("opId") String opId,
        @JsonProperty ("hostname") String hostname,
        @JsonProperty ("stage") FailoverStage stage,
        @JsonProperty ("fenceVersion") long fenceVersion,
        @JsonProperty ("ok") boolean ok,
        @JsonProperty ("errorMsg") String errorMsg
    ) {
        this.opId = Objects.requireNonNull(opId, "opId");
        this.hostname = Objects.requireNonNull(hostname, "hostname");
        this.stage = Objects.requireNonNull(stage, "stage");
        this.fenceVersion = fenceVersion;
        this.ok = ok;
        this.errorMsg = errorMsg;
    }

    public static FailoverStatusUpdate success(String opId, String hostname, FailoverStage stage, long fenceVersion) {
        return new FailoverStatusUpdate(opId, hostname, stage, fenceVersion, true, null);
    }

    public static FailoverStatusUpdate failure(
        String opId,
        String hostname,
        FailoverStage stage,
        long fenceVersion,
        String errorMsg
    ) {
        return new FailoverStatusUpdate(opId, hostname, stage, fenceVersion, false, errorMsg);
    }
}
