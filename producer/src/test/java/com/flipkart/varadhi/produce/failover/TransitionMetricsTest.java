package com.flipkart.varadhi.produce.failover;

import com.flipkart.varadhi.entities.cluster.failover.TransitionParticipation;
import com.flipkart.varadhi.entities.cluster.failover.TransitionStage;
import com.flipkart.varadhi.entities.cluster.failover.TransitionType;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TransitionMetricsTest {

    private static final String TOPIC_A = "proj.topic-a";
    private static final String TOPIC_B = "proj.topic-b";

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
    void stageFailureTopics_stickySetSizeGauge() {
        assertEquals(0.0, registry.find("topic.transition.stage.failure.topics").gauge().value());

        metrics.stageAcked(TransitionType.TOPIC_FAILOVER, TransitionStage.PREPARE, TOPIC_A, false);
        assertEquals(1.0, registry.find("topic.transition.stage.failure.topics").gauge().value());
        assertTrue(metrics.getTopicsWithStageFailure().contains(TOPIC_A));

        metrics.stageAcked(TransitionType.TOPIC_FAILOVER, TransitionStage.PREPARE, TOPIC_B, false);
        assertEquals(2.0, registry.find("topic.transition.stage.failure.topics").gauge().value());

        // Success clears sticky entry for that topic only (SqArchivalStats pattern).
        metrics.stageAcked(TransitionType.TOPIC_FAILOVER, TransitionStage.PREPARE, TOPIC_A, true);
        assertEquals(1.0, registry.find("topic.transition.stage.failure.topics").gauge().value());
        assertEquals(
            1.0,
            registry.find("topic.transition.stage.acked")
                    .tag("type", "TOPIC_FAILOVER")
                    .tag("stage", "PREPARE")
                    .counter()
                    .count()
        );
    }

    @Test
    void topicStage_mapSizeAndPerStageCounts() {
        metrics.setTopicStage(TOPIC_A, TransitionStage.PREPARE);
        metrics.setTopicStage(TOPIC_B, TransitionStage.SWITCH);

        assertEquals(2.0, registry.find("topic.transition.topics.in_flight").gauge().value());
        assertEquals(1.0, registry.find("topic.transition.topics.in_stage").tag("stage", "PREPARE").gauge().value());
        assertEquals(1.0, registry.find("topic.transition.topics.in_stage").tag("stage", "SWITCH").gauge().value());

        metrics.clearTopicStage(TOPIC_A);
        assertEquals(1.0, registry.find("topic.transition.topics.in_flight").gauge().value());
        assertEquals(0.0, registry.find("topic.transition.topics.in_stage").tag("stage", "PREPARE").gauge().value());
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
    void ackSendFailed_setsGaugeUntilSucceeded() {
        metrics.ackSendFailed(TransitionType.TOPIC_FAILOVER, TransitionStage.PREPARE);
        assertEquals(
            1.0,
            registry.find("topic.transition.ack.send.failed")
                    .tag("type", "TOPIC_FAILOVER")
                    .tag("stage", "PREPARE")
                    .gauge()
                    .value()
        );

        metrics.ackSendSucceeded(TransitionType.TOPIC_FAILOVER, TransitionStage.PREPARE);
        assertEquals(
            0.0,
            registry.find("topic.transition.ack.send.failed")
                    .tag("type", "TOPIC_FAILOVER")
                    .tag("stage", "PREPARE")
                    .gauge()
                    .value()
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
        metrics.stageAcked(TransitionType.TOPIC_FAILOVER, TransitionStage.PREPARE, TOPIC_A, false);
        metrics.setTopicStage(TOPIC_A, TransitionStage.PREPARE);
        metrics.versionWaitStarted();
        metrics.setParticipation(TransitionType.TOPIC_FAILOVER, TransitionParticipation.INVOLVED);

        metrics.close();

        assertEquals(0, registry.find("topic.transition.stage.received").counters().size());
        assertEquals(0, registry.find("topic.transition.stage.failure.topics").gauges().size());
        assertEquals(0, registry.find("topic.transition.topics.in_flight").gauges().size());
        assertEquals(0, registry.find("topic.transition.version_waits.in_flight").gauges().size());
        assertEquals(0, registry.find("topic.transition.participation").gauges().size());
    }
}
