package com.flipkart.varadhi.produce.failover;

import com.flipkart.varadhi.entities.cluster.failover.TransitionStage;
import com.flipkart.varadhi.entities.cluster.failover.TransitionType;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Micrometer-backed {@link TransitionMetrics}. Emits low-cardinality counters tagged by
 * transition type and stage so operators can track transition progress and failures per pod:
 *
 * <ul>
 *   <li>{@code topic.transition.stage.received} — tags: {@code type}, {@code stage}</li>
 *   <li>{@code topic.transition.stage.acked} — tags: {@code type}, {@code stage}, {@code success}</li>
 *   <li>{@code topic.transition.prepare.not_involved} — tags: {@code type}</li>
 * </ul>
 *
 * <p>Counters are looked up (and lazily created) per emission via the registry, which dedups by
 * name+tags; stage events are rare, so this is not on a hot path.
 */
public final class TransitionMetricsImpl implements TransitionMetrics {

    private static final String STAGE_RECEIVED = "topic.transition.stage.received";
    private static final String STAGE_ACKED = "topic.transition.stage.acked";
    private static final String PREPARE_NOT_INVOLVED = "topic.transition.prepare.not_involved";

    private final MeterRegistry registry;

    public TransitionMetricsImpl(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void stageReceived(TransitionType type, TransitionStage stage) {
        registry.counter(STAGE_RECEIVED, "type", type.name(), "stage", stage.name()).increment();
    }

    @Override
    public void stageAcked(TransitionType type, TransitionStage stage, boolean success) {
        registry.counter(STAGE_ACKED, "type", type.name(), "stage", stage.name(), "success", Boolean.toString(success))
                .increment();
    }

    @Override
    public void prepareNotInvolved(TransitionType type) {
        registry.counter(PREPARE_NOT_INVOLVED, "type", type.name()).increment();
    }
}
