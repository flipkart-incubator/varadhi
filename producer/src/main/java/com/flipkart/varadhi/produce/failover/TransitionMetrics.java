package com.flipkart.varadhi.produce.failover;

import com.flipkart.varadhi.entities.cluster.failover.TransitionParticipation;
import com.flipkart.varadhi.entities.cluster.failover.TransitionStage;
import com.flipkart.varadhi.entities.cluster.failover.TransitionType;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Micrometer metrics for the pod-side topic-transition handler.
 *
 * <p>Stage events use counters tagged by {@code type}, {@code stage}, {@code success}, and
 * {@code topic} (topic FQN). Per-topic participation is a settable gauge (oncall
 * {@code varadhi_failover_pod_node_status} style): set at PREPARE, cleared on COMPLETED/ABORTED
 * so alerts auto-resolve when the op finishes. In-flight version waits expose a global gauge and
 * a per-topic gauge.
 */
public final class TransitionMetrics {

    private static final String STAGE_RECEIVED = "topic.transition.stage.received";
    private static final String STAGE_ACKED = "topic.transition.stage.acked";
    private static final String PARTICIPATION = "topic.transition.participation";
    private static final String ACK_SEND_FAILED = "topic.transition.ack.send.failed";
    private static final String VERSION_WAITS_IN_FLIGHT = "topic.transition.version_waits.in_flight";

    private final MeterRegistry registry;
    private final AtomicInteger versionWaitsInFlight = new AtomicInteger();
    private final ConcurrentMap<String, AtomicInteger> gaugeHolders = new ConcurrentHashMap<>();

    public TransitionMetrics(MeterRegistry registry) {
        this.registry = registry;
        registry.gauge(VERSION_WAITS_IN_FLIGHT, versionWaitsInFlight);
    }

    /** A stage broadcast was received by this pod. */
    public void stageReceived(TransitionType type, TransitionStage stage, String topicFqn) {
        registry.counter(STAGE_RECEIVED, "type", type.name(), "stage", stage.name(), "topic", topicFqn).increment();
    }

    /** This pod acked a stage; {@code success} is the ack outcome. */
    public void stageAcked(TransitionType type, TransitionStage stage, boolean success, String topicFqn) {
        registry.counter(
            STAGE_ACKED,
            "type",
            type.name(),
            "stage",
            stage.name(),
            "success",
            Boolean.toString(success),
            "topic",
            topicFqn
        ).increment();
    }

    /**
     * Records this pod's participation for an in-flight op ({@code 1} on the active value,
     * {@code 0} on the other). Cleared via {@link #clearParticipation(TransitionType, String)}.
     */
    public void setParticipation(TransitionType type, String topicFqn, TransitionParticipation participation) {
        for (TransitionParticipation value : TransitionParticipation.values()) {
            setGauge(
                PARTICIPATION,
                value == participation ? 1 : 0,
                "type",
                type.name(),
                "topic",
                topicFqn,
                "participation",
                value.name()
            );
        }
    }

    /** Clears participation gauges for {@code topicFqn} when the op reaches a terminal stage. */
    public void clearParticipation(TransitionType type, String topicFqn) {
        for (TransitionParticipation value : TransitionParticipation.values()) {
            setGauge(PARTICIPATION, 0, "type", type.name(), "topic", topicFqn, "participation", value.name());
        }
    }

    /** A PREPARE resolved to NOT_INVOLVED on this pod. */
    public void prepareNotInvolved(TransitionType type) {
        registry.counter("topic.transition.prepare.not_involved", "type", type.name()).increment();
    }

    /** Failed to deliver a {@code TransitionAck} to the controller. */
    public void ackSendFailed(TransitionType type, TransitionStage stage, String topicFqn) {
        registry.counter(ACK_SEND_FAILED, "type", type.name(), "stage", stage.name(), "topic", topicFqn).increment();
    }

    /** A version-gated wait started on this pod. */
    public void versionWaitStarted(String topicFqn) {
        versionWaitsInFlight.incrementAndGet();
        versionWaitsByTopic(topicFqn).incrementAndGet();
    }

    /** A version-gated wait finished (success, failure, or timeout). */
    public void versionWaitFinished(String topicFqn) {
        versionWaitsInFlight.decrementAndGet();
        versionWaitsByTopic(topicFqn).decrementAndGet();
    }

    private AtomicInteger versionWaitsByTopic(String topicFqn) {
        return gaugeHolders.computeIfAbsent(VERSION_WAITS_IN_FLIGHT + "|topic|" + topicFqn, ignored -> {
            AtomicInteger ref = new AtomicInteger();
            registry.gauge(VERSION_WAITS_IN_FLIGHT, Tags.of("topic", topicFqn), ref, AtomicInteger::get);
            return ref;
        });
    }

    private void setGauge(String name, int value, String... tagKeyValues) {
        Tags tags = Tags.of(tagKeyValues);
        String cacheKey = name + tags;
        AtomicInteger holder = gaugeHolders.computeIfAbsent(cacheKey, ignored -> {
            AtomicInteger ref = new AtomicInteger();
            registry.gauge(name, tags, ref, AtomicInteger::get);
            return ref;
        });
        holder.set(value);
    }

    /** No-op instance for use in tests. */
    public static final TransitionMetrics NOOP = new TransitionMetrics(new SimpleMeterRegistry());
}
