package com.flipkart.varadhi.entities.cluster.failover;

/**
 * Cluster-wide stage of a topic transition (failover). The controller drives a
 * {@code TransitionObject} through these stages; only {@link #PREPARE} and
 * {@link #SWITCH} require a per-pod acknowledgment barrier.
 *
 * <p>This enum is part of the pod-facing <b>wire contract</b> (it travels inside
 * {@link FailoverStageEvent} and {@link FailoverStatusUpdate}). It carries no
 * controller-only state.
 */
public enum FailoverStage {
    PENDING, PREPARE, SWITCH, DRAIN, COMPLETED, ABORTED;

    /** Stages for which the controller waits on a per-pod ack barrier. */
    public boolean requiresAck() {
        return this == PREPARE || this == SWITCH;
    }

    public boolean isTerminal() {
        return this == COMPLETED || this == ABORTED;
    }
}
