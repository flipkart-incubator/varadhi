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
 * </ul>
 */
public record FailoverStageEvent(
    String opId,
    String topicFqn,
    String parentKind,
    FailoverStage stage,
    long topicVersionToAwait
) {

    public static final String PARENT_KIND_TOPIC = "topic";

    /**
     * PREPARE readiness: pods confirm they are alive and caught up to the current topic
     * version (N) before the switch is applied. A pod that is unreachable or stale fails
     * this barrier, letting the controller abort with no change applied.
     */
    public static FailoverStageEvent forPrepare(String opId, String topicFqn, long currentTopicVersion) {
        return new FailoverStageEvent(opId, topicFqn, PARENT_KIND_TOPIC, FailoverStage.PREPARE, currentTopicVersion);
    }

    /** SWITCH: pods wait until TopicCache reaches {@code topicVersionToAwait} (= N+1), then ack. */
    public static FailoverStageEvent forSwitch(String opId, String topicFqn, long topicVersionToAwait) {
        return new FailoverStageEvent(opId, topicFqn, PARENT_KIND_TOPIC, FailoverStage.SWITCH, topicVersionToAwait);
    }

    /**
     * Any non-version-gated stage ({@link FailoverStage#PENDING}, {@link FailoverStage#DRAIN},
     * {@link FailoverStage#COMPLETED}, {@link FailoverStage#ABORTED}): the pod has no version
     * to await and acks immediately on receipt.
     */
    public static FailoverStageEvent forStage(String opId, String topicFqn, FailoverStage stage) {
        return new FailoverStageEvent(opId, topicFqn, PARENT_KIND_TOPIC, stage, 0L);
    }
}
