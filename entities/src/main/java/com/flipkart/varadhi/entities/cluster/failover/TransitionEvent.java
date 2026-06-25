package com.flipkart.varadhi.entities.cluster.failover;

import com.flipkart.varadhi.entities.VaradhiTopicName;

import java.util.Objects;

/**
 * Immutable, self-contained payload the controller broadcasts to every pod when a topic
 * transition advances to a stage that needs pod participation. The same event drives every
 * {@link TransitionType} (topic failover, storage-topic migration); the pod reacts to
 * {@link #stage()} generically and only the {@link #target()} meaning and the PREPARE work
 * differ per type.
 *
 * <p><b>Self-contained by design:</b> a pod reacts using only the fields here plus what it
 * already has in its local {@code TopicCache}. It never reads the controller-side
 * {@code TransitionObject} (which is not replicated to pods).
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
 *       its TopicCache before acking, applied only when {@code awaitVersion} is {@code true}.
 *       For {@link TransitionStage#PREPARE} this is the current version (N) (readiness check);
 *       for {@link TransitionStage#SWITCH} it is the post-switch version (N+1). It may still be
 *       carried (for diagnostics) on non-gated stages, but is only validated and awaited when
 *       {@code awaitVersion} is set.</li>
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
        // The version is only validated when the stage actually gates on it; non-gated stages may
        // carry any value (it is ignored).
        if (awaitVersion && topicVersionToAwait <= 0) {
            throw new IllegalArgumentException("a version-gated stage requires a positive topicVersionToAwait");
        }
        if (stage == TransitionStage.PREPARE && target == null) {
            throw new IllegalArgumentException("PREPARE requires a non-null target to pre-warm");
        }
    }

    /**
     * PREPARE readiness: pods confirm they are alive and caught up to the current topic version
     * (N), and pre-create the {@code target} producer for {@code transitionType}, before the
     * switch is applied. A pod that is unreachable, stale, or cannot warm the target producer
     * fails this barrier, letting the controller abort with no change applied.
     */
    public static TransitionEvent forPrepare(
        String opId,
        VaradhiTopicName topicFqn,
        long currentTopicVersion,
        String target,
        TransitionType transitionType
    ) {
        return new TransitionEvent(
            opId,
            topicFqn,
            transitionType,
            TransitionStage.PREPARE,
            true,
            currentTopicVersion,
            target
        );
    }

    /** SWITCH: pods wait until TopicCache reaches {@code topicVersionToAwait} (= N+1), then ack. */
    public static TransitionEvent forSwitch(
        String opId,
        VaradhiTopicName topicFqn,
        long topicVersionToAwait,
        TransitionType transitionType
    ) {
        return new TransitionEvent(
            opId,
            topicFqn,
            transitionType,
            TransitionStage.SWITCH,
            true,
            topicVersionToAwait,
            null
        );
    }

    /**
     * Any non-version-gated stage ({@link TransitionStage#PENDING},
     * {@link TransitionStage#COMPLETED}, {@link TransitionStage#ABORTED}): the pod has no version
     * to await and acks immediately on receipt.
     */
    public static TransitionEvent forStage(
        String opId,
        VaradhiTopicName topicFqn,
        TransitionStage stage,
        TransitionType transitionType
    ) {
        return new TransitionEvent(opId, topicFqn, transitionType, stage, false, 0L, null);
    }
}
