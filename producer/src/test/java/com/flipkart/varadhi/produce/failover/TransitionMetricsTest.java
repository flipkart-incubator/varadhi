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
    void stageReceived_incrementsCounter() {
        metrics.stageReceived(TransitionType.TOPIC_FAILOVER, TransitionStage.PREPARE);

        assertEquals(
            1.0,
            registry.find("topic.transition.stage.received")
                    .tag("type", "TOPIC_FAILOVER")
                    .tag("stage", "PREPARE")
                    .counter()
                    .count()
        );
    }

    @Test
    void stageAcked_incrementsCounterWithSuccessTags() {
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
    void participationGauge_setAndClear() {
        metrics.setParticipation(TransitionType.STORAGE_MIGRATION, TransitionParticipation.INVOLVED);

        assertEquals(
            1.0,
            registry.find("topic.transition.participation")
                    .tag("type", "STORAGE_MIGRATION")
                    .tag("participation", "INVOLVED")
                    .gauge()
                    .value()
        );
        assertEquals(
            0.0,
            registry.find("topic.transition.participation")
                    .tag("type", "STORAGE_MIGRATION")
                    .tag("participation", "NOT_INVOLVED")
                    .gauge()
                    .value()
        );

        metrics.clearParticipation(TransitionType.STORAGE_MIGRATION);

        assertEquals(
            0.0,
            registry.find("topic.transition.participation")
                    .tag("type", "STORAGE_MIGRATION")
                    .tag("participation", "INVOLVED")
                    .gauge()
                    .value()
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
    void versionWaitGauge_tracksGlobalInFlight() {
        metrics.versionWaitStarted();
        metrics.versionWaitStarted();

        assertEquals(2.0, registry.find("topic.transition.version_waits.in_flight").gauge().value());

        metrics.versionWaitFinished();
        assertEquals(1.0, registry.find("topic.transition.version_waits.in_flight").gauge().value());
    }

    @Test
    void close_removesRegisteredMeters() {
        metrics.stageReceived(TransitionType.TOPIC_FAILOVER, TransitionStage.PREPARE);
        metrics.versionWaitStarted();
        metrics.setParticipation(TransitionType.TOPIC_FAILOVER, TransitionParticipation.INVOLVED);

        metrics.close();

        assertEquals(0, registry.find("topic.transition.stage.received").counters().size());
        assertEquals(0, registry.find("topic.transition.version_waits.in_flight").gauges().size());
        assertEquals(0, registry.find("topic.transition.participation").gauges().size());
    }
}
