package com.flipkart.varadhi.controller.failover;

import com.flipkart.varadhi.entities.cluster.failover.TransitionStage;
import com.flipkart.varadhi.entities.cluster.failover.TransitionType;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Micrometer metrics for controller-side topic-transition ack handling.
 *
 * <p>Low-cardinality tagged counters only ({@code type}, {@code stage}). Topic identity stays in logs.
 */
public final class TopicTransitionMetrics {

    private static final String ACK_RECEIVED = "topic.transition.ack.received";
    private static final String ACK_PROCESSED = "topic.transition.ack.processed";
    private static final String ACK_DELIVERY_FAILED = "topic.transition.ack.delivery.failed";

    private final MeterRegistry registry;

    public TopicTransitionMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    /** A pod {@code TransitionAck} was received on the controller route. */
    public void ackReceived(TransitionType type, TransitionStage stage) {
        increment(ACK_RECEIVED, type, stage);
    }

    /** A pod {@code TransitionAck} was handled successfully by the controller. */
    public void ackProcessed(TransitionType type, TransitionStage stage) {
        increment(ACK_PROCESSED, type, stage);
    }

    /** Failed to handle a pod {@code TransitionAck} on the controller. */
    public void ackDeliveryFailed(TransitionType type, TransitionStage stage) {
        increment(ACK_DELIVERY_FAILED, type, stage);
    }

    private void increment(String name, TransitionType type, TransitionStage stage) {
        registry.counter(name, "type", type.name(), "stage", stage.name()).increment();
    }
}
