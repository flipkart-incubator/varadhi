package com.flipkart.varadhi.controller.failover;

import com.flipkart.varadhi.entities.cluster.failover.TransitionStage;
import com.flipkart.varadhi.entities.cluster.failover.TransitionType;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Micrometer metrics for controller-side topic-transition ack handling.
 *
 * <p>Low-cardinality tagged gauges only ({@code type}, {@code stage}). Topic identity stays in logs.
 */
public final class TopicTransitionMetrics {

    private static final String ACK_RECEIVED = "topic.transition.ack.received";
    private static final String ACK_PROCESSED = "topic.transition.ack.processed";
    private static final String ACK_DELIVERY_FAILED = "topic.transition.ack.delivery.failed";

    private final MeterRegistry registry;
    private final ConcurrentMap<String, AtomicInteger> gaugeValues = new ConcurrentHashMap<>();

    public TopicTransitionMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    /** A pod {@code TransitionAck} was received on the controller route. */
    public void ackReceived(TransitionType type, TransitionStage stage) {
        incrementGauge(ACK_RECEIVED, "type", type.name(), "stage", stage.name());
    }

    /** A pod {@code TransitionAck} was handled successfully by the controller. */
    public void ackProcessed(TransitionType type, TransitionStage stage) {
        incrementGauge(ACK_PROCESSED, "type", type.name(), "stage", stage.name());
    }

    /** Failed to handle a pod {@code TransitionAck} on the controller. */
    public void ackDeliveryFailed(TransitionType type, TransitionStage stage) {
        incrementGauge(ACK_DELIVERY_FAILED, "type", type.name(), "stage", stage.name());
    }

    private void incrementGauge(String name, String... tagKeyValues) {
        Tags tags = Tags.of(tagKeyValues);
        String cacheKey = name + tags;
        AtomicInteger existing = gaugeValues.get(cacheKey);
        if (existing != null) {
            existing.incrementAndGet();
            return;
        }
        AtomicInteger created = new AtomicInteger();
        AtomicInteger prior = gaugeValues.putIfAbsent(cacheKey, created);
        AtomicInteger ref = prior != null ? prior : created;
        if (prior == null) {
            registry.gauge(name, tags, created, AtomicInteger::get);
        }
        ref.incrementAndGet();
    }
}
