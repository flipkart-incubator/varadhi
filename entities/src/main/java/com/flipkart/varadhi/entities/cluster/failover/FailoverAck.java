package com.flipkart.varadhi.entities.cluster.failover;

/**
 * Immutable pod-to-controller acknowledgment for a single {@link FailoverStage} of a
 * transition. Sent by each pod after it has applied (or failed to apply) the stage
 * broadcast in a {@link FailoverEvent}.
 *
 * <p>The controller matches an ack to its current stage barrier by {@code (opId, stage)}.
 */
public record FailoverAck(String opId, String hostname, FailoverStage stage, boolean success, String errorMsg) {
    public static FailoverAck success(String opId, String hostname, FailoverStage stage) {
        return new FailoverAck(opId, hostname, stage, true, null);
    }

    public static FailoverAck failure(String opId, String hostname, FailoverStage stage, String errorMsg) {
        return new FailoverAck(opId, hostname, stage, false, errorMsg);
    }
}
