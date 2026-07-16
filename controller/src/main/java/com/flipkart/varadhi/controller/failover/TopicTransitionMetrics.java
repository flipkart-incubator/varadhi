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

    private static final String ACK_PROCESSING_FAILED = "topic.transition.ack.processing.failed";

    private final MeterRegistry registry;
    private final ConcurrentMap<String, AtomicInteger> gaugeValues = new ConcurrentHashMap<>();

    public TopicTransitionMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    /** Failed to process a pod {@code TransitionAck} on the controller. */
    public void ackProcessingFailed(TransitionType type, TransitionStage stage) {
        incrementGauge(ACK_PROCESSING_FAILED, "type", type.name(), "stage", stage.name());
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
