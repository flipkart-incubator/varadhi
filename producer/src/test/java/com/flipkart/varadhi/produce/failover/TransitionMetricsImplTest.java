package com.flipkart.varadhi.produce.failover;

import com.flipkart.varadhi.entities.cluster.failover.TransitionStage;
import com.flipkart.varadhi.entities.cluster.failover.TransitionType;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TransitionMetricsImplTest {

    private static final String TOPIC = "project.topic";

    private SimpleMeterRegistry registry;
    private TransitionMetrics metrics;

    @BeforeEach
    void setup() {
        registry = new SimpleMeterRegistry();
        metrics = new TransitionMetrics(registry);
    }

    @Test
    void stageReceived_incrementsCounterWithTypeAndStageTags() {
        metrics.stageReceived(TransitionType.TOPIC_FAILOVER, TransitionStage.PREPARE, TOPIC);

        Counter counter = registry.find("topic.transition.stage.received")
                                  .tag("type", "TOPIC_FAILOVER")
                                  .tag("stage", "PREPARE")
                                  .tag("topic", TOPIC)
                                  .counter();
        assertEquals(1.0, counter.count());
    }

    @Test
    void stageAcked_incrementsCounterWithSuccessTag() {
        metrics.stageAcked(TransitionType.TOPIC_FAILOVER, TransitionStage.SWITCH, true, TOPIC);
        metrics.stageAcked(TransitionType.TOPIC_FAILOVER, TransitionStage.SWITCH, false, TOPIC);

        assertEquals(
            1.0,
            registry.find("topic.transition.stage.acked")
                    .tag("type", "TOPIC_FAILOVER")
                    .tag("stage", "SWITCH")
                    .tag("success", "true")
                    .tag("topic", TOPIC)
                    .counter()
                    .count()
        );
        assertEquals(
            1.0,
            registry.find("topic.transition.stage.acked")
                    .tag("type", "TOPIC_FAILOVER")
                    .tag("stage", "SWITCH")
                    .tag("success", "false")
                    .tag("topic", TOPIC)
                    .counter()
                    .count()
        );
    }

    @Test
    void prepareNotInvolved_incrementsCounterWithTypeTag() {
        metrics.prepareNotInvolved(TransitionType.STORAGE_MIGRATION);

        assertEquals(
            1.0,
            registry.find("topic.transition.prepare.not_involved").tag("type", "STORAGE_MIGRATION").counter().count()
        );
    }
}
