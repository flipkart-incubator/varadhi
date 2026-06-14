package com.flipkart.varadhi.entities.cluster.failover;

/**
 * Immutable, self-contained payload the controller broadcasts to every pod when a
 * topic transition advances to a stage that needs pod participation.
 *
 * <p><b>Self-contained by design:</b> a pod reacts using only the fields here plus
 * what it already has in its local {@code TopicCache}. It never reads the
 * controller-side {@code TransitionObject} (which is not replicated to pods).
 *
 * <ul>
 *   <li>{@code topicVersionToAwait} — the {@code VaradhiTopic} version the pod must
 *       observe in its TopicCache before acking. For {@link FailoverStage#PREPARE} this
 *       is the current version (N) (readiness check); for {@link FailoverStage#SWITCH}
 *       it is the post-switch version (N+1). {@code 0} means "no version wait" — the pod
 *       acks immediately on receipt (e.g. {@link FailoverStage#PENDING},
 *       {@link FailoverStage#DRAIN}, {@link FailoverStage#COMPLETED},
 *       {@link FailoverStage#ABORTED}).</li>
 *   <li>{@code targetRegion} — the region produce is switching <em>to</em>. Carried on
 *       {@link FailoverStage#PREPARE} so the pod can pre-create that region's producer
 *       ahead of the switch (produce continues to the old region until SWITCH). The
 *       controller sources this from the {@code TransitionObject} (FTO); it is
 *       {@code null} for stages that do not pre-warm.</li>
 * </ul>
 */
public record FailoverEvent(
    String opId,
    String topicFqn,
    String parentKind,
    FailoverStage stage,
    long topicVersionToAwait,
    String targetRegion
) {

    public static final String PARENT_KIND_TOPIC = "topic";

    /**
     * PREPARE readiness: pods confirm they are alive and caught up to the current topic
     * version (N), and pre-create the {@code targetRegion} producer, before the switch is
     * applied. A pod that is unreachable, stale, or cannot warm the target producer fails
     * this barrier, letting the controller abort with no change applied.
     */
    public static FailoverEvent forPrepare(
        String opId,
        String topicFqn,
        long currentTopicVersion,
        String targetRegion
    ) {
        return new FailoverEvent(
            opId,
            topicFqn,
            PARENT_KIND_TOPIC,
            FailoverStage.PREPARE,
            currentTopicVersion,
            targetRegion
        );
    }

    /** SWITCH: pods wait until TopicCache reaches {@code topicVersionToAwait} (= N+1), then ack. */
    public static FailoverEvent forSwitch(String opId, String topicFqn, long topicVersionToAwait) {
        return new FailoverEvent(opId, topicFqn, PARENT_KIND_TOPIC, FailoverStage.SWITCH, topicVersionToAwait, null);
    }

    /**
     * Any non-version-gated stage ({@link FailoverStage#PENDING}, {@link FailoverStage#DRAIN},
     * {@link FailoverStage#COMPLETED}, {@link FailoverStage#ABORTED}): the pod has no version
     * to await and acks immediately on receipt.
     */
    public static FailoverEvent forStage(String opId, String topicFqn, FailoverStage stage) {
        return new FailoverEvent(opId, topicFqn, PARENT_KIND_TOPIC, stage, 0L, null);
    }
}
