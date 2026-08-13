package com.flipkart.varadhi.controller.failover;

import com.flipkart.varadhi.entities.cluster.failover.TransitionStage;
import com.flipkart.varadhi.entities.cluster.failover.TransitionType;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TopicTransitionMetricsTest {

    private SimpleMeterRegistry registry;
    private TopicTransitionMetrics metrics;

    @BeforeEach
    void setup() {
        registry = new SimpleMeterRegistry();
        metrics = new TopicTransitionMetrics(registry);
    }

    @Test
    void ackReceived_incrementsTaggedCounter() {
        metrics.ackReceived(TransitionType.TOPIC_FAILOVER, TransitionStage.PREPARE);
        metrics.ackReceived(TransitionType.TOPIC_FAILOVER, TransitionStage.PREPARE);

        assertEquals(
            2.0,
            registry.find("topic.transition.ack.received")
                    .tag("type", "TOPIC_FAILOVER")
                    .tag("stage", "PREPARE")
                    .counter()
                    .count()
        );
    }

    @Test
    void ackProcessed_incrementsTaggedCounter() {
        metrics.ackProcessed(TransitionType.TOPIC_FAILOVER, TransitionStage.FENCE);

        assertEquals(
            1.0,
            registry.find("topic.transition.ack.processed")
                    .tag("type", "TOPIC_FAILOVER")
                    .tag("stage", "FENCE")
                    .counter()
                    .count()
        );
    }

    @Test
    void ackDeliveryFailed_incrementsTaggedCounter() {
        metrics.ackDeliveryFailed(TransitionType.STORAGE_MIGRATION, TransitionStage.PREPARE);

        assertEquals(
            1.0,
            registry.find("topic.transition.ack.delivery.failed")
                    .tag("type", "STORAGE_MIGRATION")
                    .tag("stage", "PREPARE")
                    .counter()
                    .count()
        );
    }
}
