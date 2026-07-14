package com.flipkart.varadhi.produce.failover;

import com.flipkart.varadhi.entities.VaradhiTopicName;
import com.flipkart.varadhi.entities.cluster.failover.TransitionStage;
import com.flipkart.varadhi.entities.cluster.failover.TransitionType;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TransitionMetricsImplTest {

    private static final VaradhiTopicName TOPIC = VaradhiTopicName.of("proj", "topic");

    private SimpleMeterRegistry registry;
    private TransitionMetricsImpl metrics;

    @BeforeEach
    void setup() {
        registry = new SimpleMeterRegistry();
        metrics = new TransitionMetricsImpl(registry);
    }

    @Test
    void stageReceived_incrementsCounterWithTypeAndStageTags() {
        metrics.stageReceived(TransitionType.TOPIC_FAILOVER, TransitionStage.PREPARE, TOPIC);

        Counter counter = registry.find("topic.transition.stage.received")
                                  .tag("type", "TOPIC_FAILOVER")
                                  .tag("stage", "PREPARE")
                                  .tag("topic", "proj.topic")
                                  .counter();
        assertEquals(1.0, counter.count());
    }

    @Test
    void stageAcked_incrementsCounterWithSuccessTag() {
        metrics.stageAcked(TransitionType.TOPIC_FAILOVER, TransitionStage.SWITCH, TOPIC, true);
        metrics.stageAcked(TransitionType.TOPIC_FAILOVER, TransitionStage.SWITCH, TOPIC, false);

        assertEquals(
            1.0,
            registry.find("topic.transition.stage.acked")
                    .tag("type", "TOPIC_FAILOVER")
                    .tag("stage", "SWITCH")
                    .tag("topic", "proj.topic")
                    .tag("success", "true")
                    .counter()
                    .count()
        );
        assertEquals(
            1.0,
            registry.find("topic.transition.stage.acked")
                    .tag("type", "TOPIC_FAILOVER")
                    .tag("stage", "SWITCH")
                    .tag("topic", "proj.topic")
                    .tag("success", "false")
                    .counter()
                    .count()
        );
    }

    @Test
    void notInvolved_incrementsCounterWithTypeTag() {
        metrics.notInvolved(TransitionType.STORAGE_MIGRATION, TOPIC);

        assertEquals(
            1.0,
            registry.find("topic.transition.not_involved")
                    .tag("type", "STORAGE_MIGRATION")
                    .tag("topic", "proj.topic")
                    .counter()
                    .count()
        );
    }
}
