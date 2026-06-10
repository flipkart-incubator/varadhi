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
 *   <li>{@code fenceVersion} — monotonic per transition; pods echo it back in their
 *       ack so the controller can drop stale acks from a previous stage/attempt.</li>
 *   <li>{@code topicVersionToAwait} — for {@link FailoverStage#SWITCH}, the
 *       {@code VaradhiTopic} version (N+1) the pod must observe in its TopicCache
 *       before acking. {@code 0} means "no version wait" (e.g. PREPARE / terminal).</li>
 * </ul>
 */
public record FailoverStageEvent(
    String opId,
    String topicFqn,
    String parentKind,
    FailoverStage stage,
    long fenceVersion,
    long topicVersionToAwait,
    String targetRegion
) {

    public static final String PARENT_KIND_TOPIC = "topic";

    /**
     * PREPARE: pods in {@code targetRegion} pre-warm their producer; no topic version to await.
     * Pods in other regions have nothing to warm but still ack the stage.
     */
    public static FailoverStageEvent forPrepare(String opId, String topicFqn, long fenceVersion, String targetRegion) {
        return new FailoverStageEvent(
            opId,
            topicFqn,
            PARENT_KIND_TOPIC,
            FailoverStage.PREPARE,
            fenceVersion,
            0L,
            targetRegion
        );
    }

    /** SWITCH: pods wait until TopicCache reaches {@code topicVersionToAwait} (= N+1), then ack. */
    public static FailoverStageEvent forSwitch(
        String opId,
        String topicFqn,
        long fenceVersion,
        long topicVersionToAwait
    ) {
        return new FailoverStageEvent(
            opId,
            topicFqn,
            PARENT_KIND_TOPIC,
            FailoverStage.SWITCH,
            fenceVersion,
            topicVersionToAwait,
            null
        );
    }

    /** Terminal (COMPLETED / ABORTED) notification; pods have nothing to apply. */
    public static FailoverStageEvent forTerminal(String opId, String topicFqn, FailoverStage stage, long fenceVersion) {
        return new FailoverStageEvent(opId, topicFqn, PARENT_KIND_TOPIC, stage, fenceVersion, 0L, null);
    }
}
