package com.flipkart.varadhi.entities.cluster.failover;

/**
 * Immutable, self-contained payload the controller broadcasts to every pod when a
 * topic transition advances to a stage that needs pod participation. The same event
 * drives every {@link TransitionType} (topic failover, storage-topic migration); the
 * pod reacts to {@link #stage()} generically and only the {@link #target()} meaning
 * and the PREPARE work differ per type.
 *
 * <p><b>Self-contained by design:</b> a pod reacts using only the fields here plus
 * what it already has in its local {@code TopicCache}. It never reads the
 * controller-side {@code TransitionObject} (which is not replicated to pods).
 *
 * <ul>
 *   <li>{@code transitionType} — which transition this event belongs to. Selects the
 *       PREPARE action the pod runs and how {@code target} is interpreted.</li>
 *   <li>{@code topicVersionToAwait} — the {@code VaradhiTopic} version the pod must
 *       observe in its TopicCache before acking. For {@link FailoverStage#PREPARE} this
 *       is the current version (N) (readiness check); for {@link FailoverStage#SWITCH}
 *       it is the post-switch version (N+1). {@code 0} means "no version wait" — the pod
 *       acks immediately on receipt (e.g. {@link FailoverStage#PENDING},
 *       {@link FailoverStage#DRAIN}, {@link FailoverStage#COMPLETED},
 *       {@link FailoverStage#ABORTED}).</li>
 *   <li>{@code target} — the destination of the switch, interpreted per
 *       {@code transitionType}: the region produce is switching <em>to</em> for
 *       {@link TransitionType#TOPIC_FAILOVER}, or the destination {@code StorageTopic}
 *       id for {@link TransitionType#STORAGE_MIGRATION}. Carried on
 *       {@link FailoverStage#PREPARE} so the pod can pre-create the target producer
 *       ahead of the switch (produce continues to the old target until SWITCH). It is
 *       {@code null} for stages that do not pre-warm.</li>
 * </ul>
 */
public record FailoverEvent(
    String opId,
    String topicFqn,
    TransitionType transitionType,
    FailoverStage stage,
    long topicVersionToAwait,
    String target
) {

    /**
     * PREPARE readiness for a {@link TransitionType#TOPIC_FAILOVER}: pods confirm they are
     * alive and caught up to the current topic version (N), and pre-create the
     * {@code targetRegion} producer, before the switch is applied.
     */
    public static FailoverEvent forPrepare(
        String opId,
        String topicFqn,
        long currentTopicVersion,
        String targetRegion
    ) {
        return forPrepare(opId, topicFqn, currentTopicVersion, targetRegion, TransitionType.TOPIC_FAILOVER);
    }

    /**
     * PREPARE readiness: pods confirm they are alive and caught up to the current topic
     * version (N), and pre-create the {@code target} producer for {@code transitionType},
     * before the switch is applied. A pod that is unreachable, stale, or cannot warm the
     * target producer fails this barrier, letting the controller abort with no change
     * applied.
     */
    public static FailoverEvent forPrepare(
        String opId,
        String topicFqn,
        long currentTopicVersion,
        String target,
        TransitionType transitionType
    ) {
        return new FailoverEvent(opId, topicFqn, transitionType, FailoverStage.PREPARE, currentTopicVersion, target);
    }

    /** SWITCH for a {@link TransitionType#TOPIC_FAILOVER}: wait until TopicCache reaches N+1, then ack. */
    public static FailoverEvent forSwitch(String opId, String topicFqn, long topicVersionToAwait) {
        return forSwitch(opId, topicFqn, topicVersionToAwait, TransitionType.TOPIC_FAILOVER);
    }

    /** SWITCH: pods wait until TopicCache reaches {@code topicVersionToAwait} (= N+1), then ack. */
    public static FailoverEvent forSwitch(
        String opId,
        String topicFqn,
        long topicVersionToAwait,
        TransitionType transitionType
    ) {
        return new FailoverEvent(opId, topicFqn, transitionType, FailoverStage.SWITCH, topicVersionToAwait, null);
    }

    /** Non-version-gated stage for a {@link TransitionType#TOPIC_FAILOVER}: ack immediately on receipt. */
    public static FailoverEvent forStage(String opId, String topicFqn, FailoverStage stage) {
        return forStage(opId, topicFqn, stage, TransitionType.TOPIC_FAILOVER);
    }

    /**
     * Any non-version-gated stage ({@link FailoverStage#PENDING}, {@link FailoverStage#DRAIN},
     * {@link FailoverStage#COMPLETED}, {@link FailoverStage#ABORTED}): the pod has no version
     * to await and acks immediately on receipt.
     */
    public static FailoverEvent forStage(
        String opId,
        String topicFqn,
        FailoverStage stage,
        TransitionType transitionType
    ) {
        return new FailoverEvent(opId, topicFqn, transitionType, stage, 0L, null);
    }
}
