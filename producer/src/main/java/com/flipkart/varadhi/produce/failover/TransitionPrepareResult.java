package com.flipkart.varadhi.produce.failover;

/**
 * Outcome of a pod's PREPARE pre-warm action for a topic transition.
 *
 * <ul>
 *   <li>{@link #WARMED} — the pod was already producing this topic and pre-created the target
 *       producer ahead of the SWITCH.</li>
 *   <li>{@link #NOT_INVOLVED} — the pod was not producing this topic, so there is nothing to
 *       pre-warm. It deliberately does <em>not</em> create a producer (avoiding unnecessary
 *       producer objects on pods that never produce the topic); it will create one lazily if it
 *       ever does. The pod still acks the stage so the controller barrier completes.</li>
 * </ul>
 */
public enum TransitionPrepareResult {
    WARMED, NOT_INVOLVED
}
