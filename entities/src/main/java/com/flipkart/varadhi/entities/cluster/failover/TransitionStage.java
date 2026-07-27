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
 * <p>Typical topic-failover order: {@link #PREPARE} → {@link #DRAIN} → {@link #SWITCH} →
 * {@link #COMPLETED} (see controller {@code TopicFailoverOpExecutor}):
 * <ul>
 *   <li>{@link #PREPARE} — readiness probe: pod confirms it is alive and caught up to
 *       the current topic version (N), and warms the target producer. Lets the controller
 *       abort before applying any change if a pod is unreachable or stale.</li>
 *   <li>{@link #DRAIN} — before SWITCH, while source may still produce: when
 *       {@code waitForReplicationLagToClear} is set the controller polls
 *       {@code StorageTopicService.getReplicationLag} until caught up (or times out).
 *       Broadcast as a fleet marker; lag is not a pod ack barrier.</li>
 *   <li>{@link #SWITCH} — fence + convergence: pod confirms it observed the new topic
 *       version (N+1) so produce re-gates.</li>
 *   <li>{@link #PENDING}, {@link #COMPLETED}, {@link #ABORTED} — lifecycle markers;
 *       usually acked immediately on receipt.</li>
 * </ul>
 */
public enum TransitionStage {
    PENDING, PREPARE, DRAIN, SWITCH, COMPLETED, ABORTED;

    public boolean isTerminal() {
        return this == COMPLETED || this == ABORTED;
    }

    /**
     * Abort is honored only before SWITCH commits the tracked topic write.
     */
    public boolean isAbortable() {
        return this == PENDING || this == PREPARE || this == DRAIN;
    }

    /** Stages where the pod must observe {@code topicVersionToAwait} before acking. */
    public boolean needsTopicVersionSync() {
        return this == PREPARE || this == SWITCH;
    }
}
