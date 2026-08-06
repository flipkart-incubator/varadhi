package com.flipkart.varadhi.entities.cluster.failover;

/**
 * Whether a producer pod participates in a topic transition (failover or storage migration).
 * Decided on the pod at PREPARE and echoed on every {@link TransitionAck} for the op so the
 * controller always knows involvement without an op-store lookup.
 *
 * <ul>
 *   <li>{@link #INVOLVED} — the pod is already producing the topic and runs participant work
 *       (e.g. pre-warm).</li>
 *   <li>{@link #NOT_INVOLVED} — the pod is not producing the topic; it skips participant work
 *       (and may fence locally later) but still acks so the barrier can complete.</li>
 * </ul>
 */
public enum TransitionParticipation {
    INVOLVED, NOT_INVOLVED
}
