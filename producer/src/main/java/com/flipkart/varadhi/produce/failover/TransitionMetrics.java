package com.flipkart.varadhi.produce.failover;

import com.flipkart.varadhi.entities.cluster.failover.TransitionStage;
import com.flipkart.varadhi.entities.cluster.failover.TransitionType;
import io.micrometer.core.instrument.*;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Micrometer metrics for the pod-side topic-transition handler.
 *
 * <p><b>Counters</b> (low-cardinality {@code type}/{@code stage}): produce ↔ controller traffic
 * — stages received/acked, ack-send outcomes.
 *
 * <p><b>Gauges</b> bind suppliers over live collections (same pattern as oncall
 * {@code SqArchivalStats}): the meter holds a ref to the set/map; scrapers call
 * {@code size()}/{@code get} — events only mutate the collection.
 * <ul>
 *   <li>Sticky produce/stage failure alert — set of topic FQNs; gauge = set size</li>
 *   <li>In-flight topic stage — map topic → stage; gauge = map size + per-stage counts</li>
 * </ul>
 */
public final class TransitionMetrics {

    /** Bus Metrics */
    private static final String STAGE_RECEIVED = "topic.transition.event.received";
    private static final String STAGE_ACKED = "topic.transition.event.acked";
    private static final String ACK_SEND_FAILED = STAGE_ACKED + ".failed";

    /** Transition Metrics */
    private static final String TOPICS_IN_STAGE = "topic.transition.active";
    private static final String STAGE_FAILURE_TOPICS = "topic.transition.failure";

    private final MeterRegistry registry;
    private final Set<Meter> registeredMeters = ConcurrentHashMap.newKeySet();

    /** Sticky: topic FQNs with a stage/produce failure until a later success clears them. */
    private final Set<String> topicsWithStageFailure = ConcurrentHashMap.newKeySet();
    /** Live stage per topic FQN on this pod. */
    private final ConcurrentMap<String, TransitionStage> topicStage = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Boolean> ackSendFailedByKey = new ConcurrentHashMap<>();

    public TransitionMetrics(MeterRegistry registry) {
        this.registry = registry;
        // Gauge holds a ref to the set/map; scrapers read size — same as SqArchivalStats.
        track(Gauge.builder(STAGE_FAILURE_TOPICS, topicsWithStageFailure, Set::size).register(registry));
        for (TransitionStage stage : TransitionStage.values()) {
            track(
                Gauge.builder(TOPICS_IN_STAGE, () -> topicStage.values().stream().filter(s -> s == stage).count())
                     .tags(Tags.of("stage", stage.name()))
                     .register(registry)
            );
        }
        for (TransitionType type : TransitionType.values()) {
            for (TransitionStage stage : TransitionStage.values()) {
                String key = key(type, stage);
                track(
                    Gauge.builder(
                        ACK_SEND_FAILED,
                        ackSendFailedByKey,
                        map -> Boolean.TRUE.equals(map.get(key)) ? 1.0 : 0.0
                    ).tags(Tags.of("type", type.name(), "stage", stage.name())).register(registry)
                );
            }
        }
    }

    /** A stage broadcast was received by this pod (produce ↔ controller traffic). */
    public void stageReceived(TransitionType type, TransitionStage stage) {
        counter(STAGE_RECEIVED, Tags.of("type", type.name(), "stage", stage.name())).increment();
    }

    /**
     * Records that this pod is handling {@code topicFqn} at {@code stage}
     * (gauge suppliers read {@link #topicStage}).
     */
    public void setTopicStage(String topicFqn, TransitionStage stage) {
        topicStage.put(topicFqn, stage);
    }

    /** Drops in-flight stage tracking for {@code topicFqn} (terminal stages). */
    public void clearTopicStage(String topicFqn) {
        topicStage.remove(topicFqn);
    }

    /**
     * Stage ack outcome. Success increments a counter and clears sticky topic failure;
     * failure adds {@code topicFqn} to the sticky set (alert on set size &gt; 0).
     * Does <em>not</em> clear the sticky set on ABORTED alone — only success removes.
     */
    public void stageAcked(TransitionType type, TransitionStage stage, String topicFqn, boolean success) {
        if (success) {
            counter(STAGE_ACKED, Tags.of("type", type.name(), "stage", stage.name())).increment();
            topicsWithStageFailure.remove(topicFqn);
        } else {
            topicsWithStageFailure.add(topicFqn);
        }
    }


    /** Failed to deliver a {@code TransitionAck} to the controller (gauge = 1 until a send succeeds). */
    public void ackSendFailed(TransitionType type, TransitionStage stage) {
        ackSendFailedByKey.put(key(type, stage), true);
    }

    /** Delivered a {@code TransitionAck} successfully — clears {@link #ackSendFailed}. */
    public void ackSendSucceeded(TransitionType type, TransitionStage stage) {
        ackSendFailedByKey.put(key(type, stage), false);
    }

    /** Snapshot of topic FQNs currently sticky-failed (for tests / diagnostics). */
    public Set<String> getTopicsWithStageFailure() {
        return Set.copyOf(topicsWithStageFailure);
    }

    /** Removes all meters this instance registered from the {@link MeterRegistry}. */
    public void close() {
        registeredMeters.forEach(registry::remove);
        registeredMeters.clear();
        topicsWithStageFailure.clear();
        topicStage.clear();
        ackSendFailedByKey.clear();
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
