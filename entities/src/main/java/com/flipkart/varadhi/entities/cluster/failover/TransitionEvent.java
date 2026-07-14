package com.flipkart.varadhi.entities.cluster.failover;

import com.flipkart.varadhi.entities.VaradhiTopicName;

import java.util.Objects;

/**
 * Immutable, self-contained payload the controller broadcasts to every pod when a topic
 * transition advances to a stage that needs pod participation. The same event drives every
 * {@link TransitionType} (topic failover, storage-topic migration); the pod reacts to
 * {@link #stage()} generically.
 *
 * <p><b>Self-contained by design:</b> a pod reacts using only the fields here plus what it
 * already has in its local {@code TopicCache}.
 *
 * <ul>
 *   <li>{@code transitionType} — which transition this event belongs to. Selects the
 *       PREPARE action the pod runs and how {@code target} is interpreted.</li>
 *   <li>{@code awaitVersion} — whether this stage is version-gated. When {@code true} the pod
 *       must observe {@code topicVersionToAwait} in its TopicCache before acking; when
 *       {@code false} the version carried is ignored and the pod acks immediately on receipt
 *       ({@link TransitionStage#PENDING}, {@link TransitionStage#COMPLETED},
 *       {@link TransitionStage#ABORTED}).</li>
 *   <li>{@code topicVersionToAwait} — the {@code VaradhiTopic} version the pod must observe in
 *       its TopicCache before acking.</li>
 *   <li>{@code target} — the destination of the switch, interpreted per {@code transitionType}:
 *       the region produce is switching <em>to</em> for {@link TransitionType#TOPIC_FAILOVER},
 *       or the destination {@code StorageTopic} id for {@link TransitionType#STORAGE_MIGRATION}.
 *       Carried on {@link TransitionStage#PREPARE} so the pod can pre-create the target producer
 *       ahead of the switch. It is {@code null} for stages that do not pre-warm. It is kept as an
 *       opaque {@code String} precisely because its meaning is type-specific; the type-specific
 *       PREPARE action interprets it (e.g. as a {@code RegionName}).</li>
 * </ul>
 */
public record TransitionEvent(
    String opId,
    VaradhiTopicName topicFqn,
    TransitionType transitionType,
    TransitionStage stage,
    boolean awaitVersion,
    long topicVersionToAwait,
    String target
) {

    public TransitionEvent {
        Objects.requireNonNull(opId, "opId must not be null");
        Objects.requireNonNull(topicFqn, "topicFqn must not be null");
        Objects.requireNonNull(transitionType, "transitionType must not be null");
        Objects.requireNonNull(stage, "stage must not be null");
        if (awaitVersion && topicVersionToAwait < 0) {
            throw new IllegalArgumentException("awaitVersion requires a non-negative topicVersionToAwait");
        }
    }

    /**
     * Creates a stage broadcast for pods. {@code awaitVersion}, {@code topicVersionToAwait}, and
     * {@code target} are controller-driven wire fields — the controller decides per stage whether
     * pods must converge on a version and whether a pre-warm target is carried.
     */
    public static TransitionEvent of(
        String opId,
        VaradhiTopicName topicFqn,
        TransitionType transitionType,
        TransitionStage stage,
        boolean awaitVersion,
        long topicVersionToAwait,
        String target
    ) {
        return new TransitionEvent(opId, topicFqn, transitionType, stage, awaitVersion, topicVersionToAwait, target);
    }
}
