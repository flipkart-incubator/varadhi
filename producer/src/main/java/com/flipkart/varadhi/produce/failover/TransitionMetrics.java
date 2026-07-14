package com.flipkart.varadhi.produce.failover;

import com.flipkart.varadhi.entities.VaradhiTopicName;
import com.flipkart.varadhi.entities.cluster.failover.TransitionStage;
import com.flipkart.varadhi.entities.cluster.failover.TransitionType;

/**
 * Observability seam for the pod-side topic-transition stage handler. Lets the handler emit
 * counters for stage receipts, acks (with outcome), and not-involved participation without
 * coupling it to a specific metrics backend.
 *
 * @see TransitionMetricsImpl
 */
public interface TransitionMetrics {

    /** A stage broadcast was received by this pod. */
    void stageReceived(TransitionType type, TransitionStage stage, VaradhiTopicName topicFqn);

    /** This pod acked a stage; {@code success} is the ack outcome. */
    void stageAcked(TransitionType type, TransitionStage stage, VaradhiTopicName topicFqn, boolean success);

    /** This pod is {@link TransitionParticipation#NOT_INVOLVED} for the transition. */
    void notInvolved(TransitionType type, VaradhiTopicName topicFqn);

    TransitionMetrics NOOP = new NoOpImpl();

    class NoOpImpl implements TransitionMetrics {

        @Override
        public void stageReceived(TransitionType type, TransitionStage stage, VaradhiTopicName topicFqn) {
        }

        @Override
        public void stageAcked(TransitionType type, TransitionStage stage, VaradhiTopicName topicFqn, boolean success) {
        }

        @Override
        public void notInvolved(TransitionType type, VaradhiTopicName topicFqn) {
        }
    }
}
