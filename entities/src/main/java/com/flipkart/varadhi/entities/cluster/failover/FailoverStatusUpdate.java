package com.flipkart.varadhi.entities.cluster.failover;

/**
 * Immutable pod-to-controller acknowledgment for a single {@link FailoverStage} of a
 * transition. Sent by each pod after it has applied (or failed to apply) the stage
 * broadcast in a {@link FailoverStageEvent}.
 *
 * <p>The controller matches an ack to its current stage barrier by {@code (opId, stage)}.
 */
public record FailoverStatusUpdate(String opId, String hostname, FailoverStage stage, boolean success, String errorMsg) {
    public static FailoverStatusUpdate success(String opId, String hostname, FailoverStage stage) {
        return new FailoverStatusUpdate(opId, hostname, stage, true, null);
    }

    public static FailoverStatusUpdate failure(String opId, String hostname, FailoverStage stage, String errorMsg) {
        return new FailoverStatusUpdate(opId, hostname, stage, false, errorMsg);
    }
}
