package com.flipkart.varadhi.entities.cluster.failover;

/**
 * Cluster-wide stage of a topic transition (e.g. topic failover, storage-topic migration).
 * The controller drives a {@code TransitionMaster} through these stages and waits on a
 * per-pod ack barrier at <b>every</b> stage it broadcasts — each pod acknowledges every
 * stage so the controller can confirm fleet-wide progress and abort/remediate on a missing
 * or failed ack.
 *
 * <p>This enum is part of the pod-facing <b>wire contract</b> (it travels inside
 * {@link TransitionEvent} and {@link TransitionAck}). It carries no controller-only state.
 *
 * <p>Typical topic-failover order on the controller executor:
 * {@link #PREPARE} → {@link #DRAIN} → {@link #SWITCH} → {@link #COMPLETED}:
 * <ul>
 *   <li>{@link #PREPARE} — readiness: pod waits for topic version N and may warm the target.</li>
 *   <li>{@link #DRAIN} — optional replication-lag wait (controller); broadcast as fleet marker.</li>
 *   <li>{@link #SWITCH} — fence + convergence on topic version N′.</li>
 *   <li>{@link #PENDING}, {@link #COMPLETED}, {@link #ABORTED} — lifecycle markers.</li>
 * </ul>
 */
public enum TransitionStage {
    PENDING, PREPARE, DRAIN, SWITCH, COMPLETED, ABORTED;

    public boolean isTerminal() {
        return this == COMPLETED || this == ABORTED;
    }

    /** Abort is honored only before SWITCH commits the tracked topic write. */
    public boolean isAbortable() {
        return this == PENDING || this == PREPARE || this == DRAIN;
    }

    /** Stages where the pod must observe {@code topicVersionToAwait} before acking. */
    public boolean needsTopicVersionSync() {
        return this == PREPARE || this == SWITCH;
    }
}
