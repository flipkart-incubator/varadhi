package com.flipkart.varadhi.produce.failover;

import com.flipkart.varadhi.entities.cluster.failover.TransitionParticipation;
import com.flipkart.varadhi.entities.cluster.failover.TransitionStage;
import com.flipkart.varadhi.entities.cluster.failover.TransitionType;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TransitionMetricsTest {

    private static final String TOPIC = "proj.topic1";

    private SimpleMeterRegistry registry;
    private TransitionMetrics metrics;

    @BeforeEach
    void setup() {
        registry = new SimpleMeterRegistry();
        metrics = new TransitionMetrics(registry);
    }

    @Test
    void stageReceived_incrementsCounterWithTopicTag() {
        metrics.stageReceived(TransitionType.TOPIC_FAILOVER, TransitionStage.PREPARE, TOPIC);

        assertEquals(
            1.0,
            registry.find("topic.transition.stage.received")
                    .tag("type", "TOPIC_FAILOVER")
                    .tag("stage", "PREPARE")
                    .tag("topic", TOPIC)
                    .counter()
                    .count()
        );
    }

    @Test
    void stageAcked_incrementsCounterWithSuccessAndTopicTags() {
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
    void participationGauge_setAndClearPerTopic() {
        metrics.setParticipation(TransitionType.STORAGE_MIGRATION, TOPIC, TransitionParticipation.INVOLVED);

        assertEquals(
            1.0,
            registry.find("topic.transition.participation")
                    .tag("type", "STORAGE_MIGRATION")
                    .tag("topic", TOPIC)
                    .tag("participation", "INVOLVED")
                    .gauge()
                    .value()
        );
        assertEquals(
            0.0,
            registry.find("topic.transition.participation")
                    .tag("type", "STORAGE_MIGRATION")
                    .tag("topic", TOPIC)
                    .tag("participation", "NOT_INVOLVED")
                    .gauge()
                    .value()
        );

        metrics.clearParticipation(TransitionType.STORAGE_MIGRATION, TOPIC);

        assertEquals(
            0.0,
            registry.find("topic.transition.participation")
                    .tag("type", "STORAGE_MIGRATION")
                    .tag("topic", TOPIC)
                    .tag("participation", "INVOLVED")
                    .gauge()
                    .value()
        );
    }

    @Test
    void ackSendFailed_incrementsCounterWithTopicTag() {
        metrics.ackSendFailed(TransitionType.TOPIC_FAILOVER, TransitionStage.PREPARE, TOPIC);

        assertEquals(
            1.0,
            registry.find("topic.transition.ack.send.failed")
                    .tag("type", "TOPIC_FAILOVER")
                    .tag("stage", "PREPARE")
                    .tag("topic", TOPIC)
                    .counter()
                    .count()
        );
    }

    @Test
    void versionWaitGauge_tracksGlobalAndPerTopicInFlight() {
        metrics.versionWaitStarted(TOPIC);
        metrics.versionWaitStarted(TOPIC);

        assertEquals(2.0, registry.find("topic.transition.version_waits.in_flight").gauge().value());
        assertEquals(
            2.0,
            registry.find("topic.transition.version_waits.in_flight").tag("topic", TOPIC).gauge().value()
        );

        metrics.versionWaitFinished(TOPIC);
        assertEquals(1.0, registry.find("topic.transition.version_waits.in_flight").gauge().value());
        assertEquals(
            1.0,
            registry.find("topic.transition.version_waits.in_flight").tag("topic", TOPIC).gauge().value()
        );
    }
}
