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
    void ackProcessingFailed_incrementsTaggedGauge() {
        metrics.ackProcessingFailed(TransitionType.TOPIC_FAILOVER, TransitionStage.PREPARE);

        assertEquals(
            1.0,
            registry.find("topic.transition.ack.processing.failed")
                    .tag("type", "TOPIC_FAILOVER")
                    .tag("stage", "PREPARE")
                    .gauge()
                    .value()
        );
    }
}
