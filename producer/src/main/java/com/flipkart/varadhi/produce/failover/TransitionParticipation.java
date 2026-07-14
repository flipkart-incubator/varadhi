package com.flipkart.varadhi.produce.failover;

/**
 * Whether this producer pod participates in a topic transition (failover or storage migration).
 * Decided by {@link ProduceTransitionMsgHandler} for the lifetime of the transition.
 *
 * <ul>
 *   <li>{@link #INVOLVED} — this pod is already producing the topic and takes part in stage work
 *       (e.g. pre-warming the target producer ahead of SWITCH).</li>
 *   <li>{@link #NOT_INVOLVED} — this pod is not producing the topic, so it skips participant work
 *       (avoids creating producers it does not need; local fencing can plug in later). It still
 *       acks stages so the controller barrier can complete.</li>
 * </ul>
 */
public enum TransitionParticipation {
    INVOLVED, NOT_INVOLVED
}
