package com.flipkart.varadhi.entities.cluster.failover;

/**
 * Cluster-wide stage of a topic transition (e.g. topic failover, storage-topic migration).
 * The controller drives a {@code TransitionObject} through these stages and waits on a
 * per-pod ack barrier at <b>every</b> stage it broadcasts — each pod acknowledges every
 * stage so the controller can confirm fleet-wide progress and abort/remediate on a missing
 * or failed ack.
 *
 * <p>This enum is part of the pod-facing <b>wire contract</b> (it travels inside
 * {@link TransitionEvent} and {@link TransitionAck}). It carries no controller-only state.
 *
 * <p>Some stages are <i>version-gated</i>: the pod must observe a specific topic version
 * in its local TopicCache before acking.
 * <ul>
 *   <li>{@link #PREPARE} — readiness probe: pod confirms it is alive and caught up to
 *       the current topic version (N). Lets the controller abort before applying any
 *       change if a pod is unreachable or stale.</li>
 *   <li>{@link #SWITCH} — convergence: pod confirms it observed the new topic version
 *       (N+1) so produce re-gates to the new region.</li>
 * </ul>
 *
 * <p>All other stages ({@link #PENDING}, {@link #DRAIN}, {@link #COMPLETED},
 * {@link #ABORTED}) carry no version and are acked immediately on receipt — a
 * confirmation that the pod processed the stage.
 */
public enum TransitionStage {
    PENDING, PREPARE, SWITCH, DRAIN, COMPLETED, ABORTED;

    public boolean isTerminal() {
        return this == COMPLETED || this == ABORTED;
    }
}
