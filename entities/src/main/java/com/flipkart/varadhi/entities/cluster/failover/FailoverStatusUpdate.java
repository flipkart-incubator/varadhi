package com.flipkart.varadhi.entities.cluster.failover;

/**
 * Immutable pod-to-controller acknowledgment for a single {@link FailoverStage} of a
 * transition. Sent by each pod after it has applied (or failed to apply) the stage
 * broadcast in a {@link FailoverStageEvent}.
 *
 * <p>{@code fenceVersion} mirrors the value from the triggering event so the controller
 * can match the ack to the current stage barrier and discard stale acks.
 */
public record FailoverStatusUpdate(
    String opId,
    String hostname,
    FailoverStage stage,
    long fenceVersion,
    boolean ok,
    String errorMsg
) {
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
