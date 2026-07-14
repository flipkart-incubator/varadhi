package com.flipkart.varadhi.produce.failover;

import com.flipkart.varadhi.entities.VaradhiTopicName;
import com.flipkart.varadhi.entities.cluster.failover.TransitionStage;
import com.flipkart.varadhi.entities.cluster.failover.TransitionType;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Micrometer-backed {@link TransitionMetrics}. Emits counters tagged by transition type and stage:
 *
 * <ul>
 *   <li>{@code topic.transition.stage.received} — tags: {@code type}, {@code stage}, {@code topic}</li>
 *   <li>{@code topic.transition.stage.acked} — tags: {@code type}, {@code stage}, {@code topic}, {@code success}</li>
 *   <li>{@code topic.transition.not_involved} — tags: {@code type}, {@code topic}</li>
 * </ul>
 */
public final class TransitionMetricsImpl implements TransitionMetrics {

    private static final String STAGE_RECEIVED = "topic.transition.stage.received";
    private static final String STAGE_ACKED = "topic.transition.stage.acked";
    private static final String NOT_INVOLVED = "topic.transition.not_involved";

    private final MeterRegistry registry;

    public TransitionMetricsImpl(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void stageReceived(TransitionType type, TransitionStage stage, VaradhiTopicName topicFqn) {
        registry.counter(STAGE_RECEIVED, "type", type.name(), "stage", stage.name(), "topic", topicFqn.toFqn())
                .increment();
    }

    @Override
    public void stageAcked(TransitionType type, TransitionStage stage, VaradhiTopicName topicFqn, boolean success) {
        registry.counter(
            STAGE_ACKED,
            "type",
            type.name(),
            "stage",
            stage.name(),
            "topic",
            topicFqn.toFqn(),
            "success",
            Boolean.toString(success)
        ).increment();
    }

    @Override
    public void notInvolved(TransitionType type, VaradhiTopicName topicFqn) {
        registry.counter(NOT_INVOLVED, "type", type.name(), "topic", topicFqn.toFqn()).increment();
    }
}
