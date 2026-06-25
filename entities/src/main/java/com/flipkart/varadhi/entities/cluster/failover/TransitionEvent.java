package com.flipkart.varadhi.entities.cluster.failover;

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
 *   <li>{@code topicVersionToAwait} — the {@code VaradhiTopic} version the pod must observe in
 *       its TopicCache before acking. For {@link TransitionStage#PREPARE} this is the current
 *       version (N) (readiness check); for {@link TransitionStage#SWITCH} it is the post-switch
 *       version (N+1). {@code 0} means "no version wait" — the pod acks immediately on receipt
 *       (e.g. {@link TransitionStage#PENDING}, {@link TransitionStage#DRAIN},
 *       {@link TransitionStage#COMPLETED}, {@link TransitionStage#ABORTED}).</li>
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
    String topicFqn,
    TransitionType transitionType,
    TransitionStage stage,
    long topicVersionToAwait,
    String target
) {

    public TransitionEvent {
        if (transitionType == null) {
            throw new IllegalArgumentException("transitionType must not be null");
        }
        if (stage == null) {
            throw new IllegalArgumentException("stage must not be null");
        }
        switch (stage) {
            case PREPARE -> {
                if (topicVersionToAwait <= 0) {
                    throw new IllegalArgumentException("PREPARE requires a positive topicVersionToAwait");
                }
                if (target == null) {
                    throw new IllegalArgumentException("PREPARE requires a non-null target to pre-warm");
                }
            }
            case SWITCH -> {
                if (topicVersionToAwait <= 0) {
                    throw new IllegalArgumentException("SWITCH requires a positive topicVersionToAwait");
                }
            }
            default -> {
                if (topicVersionToAwait != 0) {
                    throw new IllegalArgumentException(stage + " must carry topicVersionToAwait 0");
                }
            }
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
        String topicFqn,
        long currentTopicVersion,
        String target,
        TransitionType transitionType
    ) {
        return new TransitionEvent(
            opId,
            topicFqn,
            transitionType,
            TransitionStage.PREPARE,
            currentTopicVersion,
            target
        );
    }

    /** SWITCH: pods wait until TopicCache reaches {@code topicVersionToAwait} (= N+1), then ack. */
    public static TransitionEvent forSwitch(
        String opId,
        String topicFqn,
        long topicVersionToAwait,
        TransitionType transitionType
    ) {
        return new TransitionEvent(opId, topicFqn, transitionType, TransitionStage.SWITCH, topicVersionToAwait, null);
    }

    /**
     * Any non-version-gated stage ({@link TransitionStage#PENDING}, {@link TransitionStage#DRAIN},
     * {@link TransitionStage#COMPLETED}, {@link TransitionStage#ABORTED}): the pod has no version
     * to await and acks immediately on receipt.
     */
    public static TransitionEvent forStage(
        String opId,
        String topicFqn,
        TransitionStage stage,
        TransitionType transitionType
    ) {
        return new TransitionEvent(opId, topicFqn, transitionType, stage, 0L, null);
    }
}
