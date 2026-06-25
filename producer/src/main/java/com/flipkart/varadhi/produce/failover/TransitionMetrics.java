package com.flipkart.varadhi.produce.failover;

import com.flipkart.varadhi.entities.cluster.failover.TransitionStage;
import com.flipkart.varadhi.entities.cluster.failover.TransitionType;

/**
 * Observability seam for the pod-side topic-transition stage handler. Lets the handler emit
 * counters for stage receipts, acks (with outcome), and not-involved PREPAREs without coupling
 * it to a specific metrics backend. Use {@link #NOOP} where metrics are not wired.
 *
 * @see TransitionMetricsImpl
 */
public interface TransitionMetrics {

    /** A stage broadcast was received by this pod. */
    void stageReceived(TransitionType type, TransitionStage stage);

    /** This pod acked a stage; {@code success} is the ack outcome. */
    void stageAcked(TransitionType type, TransitionStage stage, boolean success);

    /** A PREPARE resolved to {@link TransitionPrepareResult#NOT_INVOLVED} on this pod. */
    void prepareNotInvolved(TransitionType type);

    TransitionMetrics NOOP = new TransitionMetrics() {
        @Override
        public void stageReceived(TransitionType type, TransitionStage stage) {
        }

        @Override
        public void stageAcked(TransitionType type, TransitionStage stage, boolean success) {
        }

        @Override
        public void prepareNotInvolved(TransitionType type) {
        }
    };
}
