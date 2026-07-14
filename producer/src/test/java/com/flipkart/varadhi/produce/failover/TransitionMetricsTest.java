package com.flipkart.varadhi.produce.failover;

import com.flipkart.varadhi.entities.cluster.failover.TransitionParticipation;
import com.flipkart.varadhi.entities.cluster.failover.TransitionStage;
import com.flipkart.varadhi.entities.cluster.failover.TransitionType;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TransitionMetricsTest {

    private SimpleMeterRegistry registry;
    private TransitionMetrics metrics;

    @BeforeEach
    void setup() {
        registry = new SimpleMeterRegistry();
        metrics = new TransitionMetrics(registry);
    }

    @Test
    void stageReceived_incrementsCounterWithoutTopicTag() {
        metrics.stageReceived(TransitionType.TOPIC_FAILOVER, TransitionStage.PREPARE);

        assertEquals(
            1.0,
            registry.find("topic.transition.stage.received")
                    .tag("type", "TOPIC_FAILOVER")
                    .tag("stage", "PREPARE")
                    .counter()
                    .count()
        );
        assertEquals(0, registry.find("topic.transition.stage.received").tag("topic", "proj.topic").counters().size());
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
    void participation_incrementsCounterWithParticipationTag() {
        metrics.participation(TransitionType.STORAGE_MIGRATION, TransitionParticipation.NOT_INVOLVED);

        assertEquals(
            1.0,
            registry.find("topic.transition.participation")
                    .tag("type", "STORAGE_MIGRATION")
                    .tag("participation", "NOT_INVOLVED")
                    .counter()
                    .count()
        );
    }

    @Test
    void ackSendFailed_incrementsCounter() {
        metrics.ackSendFailed(TransitionType.TOPIC_FAILOVER, TransitionStage.PREPARE);

        assertEquals(
            1.0,
            registry.find("topic.transition.ack.send.failed")
                    .tag("type", "TOPIC_FAILOVER")
                    .tag("stage", "PREPARE")
                    .counter()
                    .count()
        );
    }

    @Test
    void versionWaitGauge_tracksInFlight() {
        metrics.versionWaitStarted();
        metrics.versionWaitStarted();
        assertEquals(2.0, registry.find("topic.transition.version_waits.in_flight").gauge().value());
        metrics.versionWaitFinished();
        assertEquals(1.0, registry.find("topic.transition.version_waits.in_flight").gauge().value());
    }
}
