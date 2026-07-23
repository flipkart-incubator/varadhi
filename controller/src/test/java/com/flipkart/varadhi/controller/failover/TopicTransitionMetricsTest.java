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
    void ackReceived_incrementsTaggedGauge() {
        metrics.ackReceived(TransitionType.TOPIC_FAILOVER, TransitionStage.PREPARE);

        assertEquals(
            1.0,
            registry.find("topic.transition.ack.received")
                    .tag("type", "TOPIC_FAILOVER")
                    .tag("stage", "PREPARE")
                    .gauge()
                    .value()
        );
    }

    @Test
    void ackProcessed_incrementsTaggedGauge() {
        metrics.ackProcessed(TransitionType.TOPIC_FAILOVER, TransitionStage.SWITCH);

        assertEquals(
            1.0,
            registry.find("topic.transition.ack.processed")
                    .tag("type", "TOPIC_FAILOVER")
                    .tag("stage", "SWITCH")
                    .gauge()
                    .value()
        );
    }

    @Test
    void ackDeliveryFailed_incrementsTaggedGauge() {
        metrics.ackDeliveryFailed(TransitionType.STORAGE_MIGRATION, TransitionStage.PREPARE);

        assertEquals(
            1.0,
            registry.find("topic.transition.ack.delivery.failed")
                    .tag("type", "STORAGE_MIGRATION")
                    .tag("stage", "PREPARE")
                    .gauge()
                    .value()
        );
    }
}
