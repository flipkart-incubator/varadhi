package com.flipkart.varadhi.produce.failover;

import com.flipkart.varadhi.entities.cluster.failover.TransitionStage;
import com.flipkart.varadhi.entities.cluster.failover.TransitionType;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TransitionMetricsImplTest {

    private SimpleMeterRegistry registry;
    private TransitionMetricsImpl metrics;

    @BeforeEach
    void setup() {
        registry = new SimpleMeterRegistry();
        metrics = new TransitionMetricsImpl(registry);
    }

    @Test
    void stageReceived_incrementsCounterWithTypeAndStageTags() {
        metrics.stageReceived(TransitionType.TOPIC_FAILOVER, TransitionStage.PREPARE);

        Counter counter = registry.find("topic.transition.stage.received")
                                  .tag("type", "TOPIC_FAILOVER")
                                  .tag("stage", "PREPARE")
                                  .counter();
        assertEquals(1.0, counter.count());
    }

    @Test
    void stageAcked_incrementsCounterWithSuccessTag() {
        metrics.stageAcked(TransitionType.TOPIC_FAILOVER, TransitionStage.SWITCH, true);
        metrics.stageAcked(TransitionType.TOPIC_FAILOVER, TransitionStage.SWITCH, false);

        assertEquals(
            1.0,
            registry.find("topic.transition.stage.acked")
                    .tag("type", "TOPIC_FAILOVER")
                    .tag("stage", "SWITCH")
                    .tag("success", "true")
                    .counter()
                    .count()
        );
        assertEquals(
            1.0,
            registry.find("topic.transition.stage.acked")
                    .tag("type", "TOPIC_FAILOVER")
                    .tag("stage", "SWITCH")
                    .tag("success", "false")
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
