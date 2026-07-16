package com.flipkart.varadhi.produce.failover;

import com.flipkart.varadhi.entities.cluster.failover.TransitionParticipation;
import com.flipkart.varadhi.entities.cluster.failover.TransitionStage;
import com.flipkart.varadhi.entities.cluster.failover.TransitionType;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Micrometer metrics for the pod-side topic-transition handler.
 *
 * <p>Event rates use low-cardinality counters ({@code type}, {@code stage}, {@code success},
 * {@code participation}). Topic identity stays in logs / {@code TransitionAck}, not metric tags.
 * In-flight version waits are exposed as a gauge.
 */
public final class TransitionMetrics {

    private static final String STAGE_RECEIVED = "topic.transition.stage.received";
    private static final String STAGE_ACKED = "topic.transition.stage.acked";
    private static final String PARTICIPATION = "topic.transition.participation";
    private static final String ACK_SEND_FAILED = "topic.transition.ack.send.failed";
    private static final String VERSION_WAITS_IN_FLIGHT = "topic.transition.version_waits.in_flight";

    private final MeterRegistry registry;
    private final AtomicInteger versionWaitsInFlight = new AtomicInteger();

    public TransitionMetrics(MeterRegistry registry) {
        this.registry = registry;
        registry.gauge(VERSION_WAITS_IN_FLIGHT, versionWaitsInFlight);
    }

    /** A stage broadcast was received by this pod. */
    public void stageReceived(TransitionType type, TransitionStage stage) {
        registry.counter(STAGE_RECEIVED, "type", type.name(), "stage", stage.name()).increment();
    }

    /** This pod acked a stage; {@code success} is the ack outcome. */
    public void stageAcked(TransitionType type, TransitionStage stage, boolean success) {
        registry.counter(STAGE_ACKED, "type", type.name(), "stage", stage.name(), "success", Boolean.toString(success))
                .increment();
    }

    /** PREPARE participation decision on this pod. */
    public void participation(TransitionType type, TransitionParticipation participation) {
        registry.counter(PARTICIPATION, "type", type.name(), "participation", participation.name()).increment();
    }

    /** Failed to deliver a {@code TransitionAck} to the controller. */
    public void ackSendFailed(TransitionType type, TransitionStage stage) {
        registry.counter(ACK_SEND_FAILED, "type", type.name(), "stage", stage.name()).increment();
    }

    /** A version-gated wait started on this pod. */
    public void versionWaitStarted() {
        versionWaitsInFlight.incrementAndGet();
    }

    /** A version-gated wait finished (success, failure, or timeout). */
    public void versionWaitFinished() {
        versionWaitsInFlight.decrementAndGet();
    }
}
