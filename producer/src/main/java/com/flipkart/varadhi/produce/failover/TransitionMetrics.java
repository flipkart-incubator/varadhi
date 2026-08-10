package com.flipkart.varadhi.produce.failover;

import com.flipkart.varadhi.entities.cluster.failover.TransitionParticipation;
import com.flipkart.varadhi.entities.cluster.failover.TransitionStage;
import com.flipkart.varadhi.entities.cluster.failover.TransitionType;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Micrometer metrics for the pod-side topic-transition handler.
 *
 * <p>Low-cardinality tags only ({@code type}, {@code stage}, {@code participation}).
 * Topic identity stays in logs.
 *
 * <p>Throughput stays on counters; recoverable failure / participation state is exposed as
 * gauges bound to suppliers over live maps so scrapers always read current state.
 */
public final class TransitionMetrics {

    private static final String STAGE_RECEIVED = "topic.transition.stage.received";
    private static final String STAGE_ACKED = "topic.transition.stage.acked";
    private static final String STAGE_ACK_FAILED = "topic.transition.stage.ack.failed";
    private static final String PARTICIPATION = "topic.transition.participation";
    private static final String ACK_SEND_FAILED = "topic.transition.ack.send.failed";
    private static final String VERSION_WAITS_IN_FLIGHT = "topic.transition.version_waits.in_flight";

    private final MeterRegistry registry;
    private final Set<Meter> registeredMeters = ConcurrentHashMap.newKeySet();
    private final AtomicInteger versionWaitsInFlight = new AtomicInteger();
    private final ConcurrentMap<TransitionType, TransitionParticipation> participationByType =
        new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Boolean> stageAckFailedByKey = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Boolean> ackSendFailedByKey = new ConcurrentHashMap<>();

    public TransitionMetrics(MeterRegistry registry) {
        this.registry = registry;
        track(Gauge.builder(VERSION_WAITS_IN_FLIGHT, versionWaitsInFlight, AtomicInteger::get).register(registry));
        for (TransitionType type : TransitionType.values()) {
            for (TransitionParticipation participation : TransitionParticipation.values()) {
                TransitionType transitionType = type;
                TransitionParticipation participationValue = participation;
                track(
                    Gauge.builder(
                        PARTICIPATION,
                        participationByType,
                        map -> map.get(transitionType) == participationValue ? 1.0 : 0.0
                    )
                         .tags(Tags.of("type", transitionType.name(), "participation", participationValue.name()))
                         .register(registry)
                );
            }
            for (TransitionStage stage : TransitionStage.values()) {
                String key = key(type, stage);
                TransitionType transitionType = type;
                TransitionStage transitionStage = stage;
                track(
                    Gauge.builder(
                        STAGE_ACK_FAILED,
                        stageAckFailedByKey,
                        map -> Boolean.TRUE.equals(map.get(key)) ? 1.0 : 0.0
                    ).tags(Tags.of("type", transitionType.name(), "stage", transitionStage.name())).register(registry)
                );
                track(
                    Gauge.builder(
                        ACK_SEND_FAILED,
                        ackSendFailedByKey,
                        map -> Boolean.TRUE.equals(map.get(key)) ? 1.0 : 0.0
                    ).tags(Tags.of("type", transitionType.name(), "stage", transitionStage.name())).register(registry)
                );
            }
        }
    }

    /** A stage broadcast was received by this pod. */
    public void stageReceived(TransitionType type, TransitionStage stage) {
        counter(STAGE_RECEIVED, Tags.of("type", type.name(), "stage", stage.name())).increment();
    }

    /**
     * This pod finished acking a stage. Success increments a throughput counter and clears the
     * failure gauge; failure marks the gauge {@code 1} (alert-friendly, clears on next success).
     */
    public void stageAcked(TransitionType type, TransitionStage stage, boolean success) {
        if (success) {
            counter(STAGE_ACKED, Tags.of("type", type.name(), "stage", stage.name())).increment();
            stageAckFailedByKey.put(key(type, stage), false);
        } else {
            stageAckFailedByKey.put(key(type, stage), true);
        }
    }

    /**
     * Records this pod's participation for an in-flight op ({@code 1} on the active value,
     * {@code 0} on the other). Cleared via {@link #clearParticipation(TransitionType)}.
     */
    public void setParticipation(TransitionType type, TransitionParticipation participation) {
        participationByType.put(type, participation);
    }

    /** Clears participation gauges when the op reaches a terminal stage. */
    public void clearParticipation(TransitionType type) {
        participationByType.remove(type);
    }

    /** Failed to deliver a {@code TransitionAck} to the controller (gauge = 1 until a send succeeds). */
    public void ackSendFailed(TransitionType type, TransitionStage stage) {
        ackSendFailedByKey.put(key(type, stage), true);
    }

    /** Delivered a {@code TransitionAck} successfully — clears {@link #ackSendFailed}. */
    public void ackSendSucceeded(TransitionType type, TransitionStage stage) {
        ackSendFailedByKey.put(key(type, stage), false);
    }

    /** A version-gated wait started on this pod. */
    public void versionWaitStarted() {
        versionWaitsInFlight.incrementAndGet();
    }

    /** A version-gated wait finished (success, failure, or timeout). */
    public void versionWaitFinished() {
        versionWaitsInFlight.decrementAndGet();
    }

    /** Removes all meters this instance registered from the {@link MeterRegistry}. */
    public void close() {
        registeredMeters.forEach(registry::remove);
        registeredMeters.clear();
        participationByType.clear();
        stageAckFailedByKey.clear();
        ackSendFailedByKey.clear();
        versionWaitsInFlight.set(0);
    }

    private static String key(TransitionType type, TransitionStage stage) {
        return type.name() + "|" + stage.name();
    }

    private Counter counter(String name, Tags tags) {
        return track(Counter.builder(name).tags(tags).register(registry));
    }

    private <T extends Meter> T track(T meter) {
        registeredMeters.add(meter);
        return meter;
    }
}
