package com.flipkart.varadhi.entities.cluster.failover;

import com.flipkart.varadhi.entities.VaradhiTopicName;

import java.util.Objects;

/**
 * Immutable pod-to-controller acknowledgment for a single {@link TransitionStage} of a
 * topic transition. Sent by each pod after it has applied (or failed to apply) the stage
 * broadcast in a {@link TransitionEvent}.
 *
 * <p>The controller matches an ack to its current stage barrier by {@code (opId, stage)}.
 * {@code topicFqn} and {@code transitionType} are echoed from the event so logs and metrics
 * remain self-describing without an op-store lookup.
 *
 * <p>{@code errorMsg} is the single source of truth for outcome: it is {@code null}/blank on
 * success and a non-blank reason on failure. {@link #isSuccess()} is derived from it so the two
 * can never disagree.
 *
 * @param opId           the transition operation id this ack belongs to
 * @param topicFqn       the topic the transition is for
 * @param transitionType which transition this ack belongs to
 * @param hostname       the acking pod's hostname
 * @param stage          the stage being acknowledged
 * @param errorMsg       {@code null} (or blank) on success; a non-blank failure reason otherwise
 */
public record TransitionAck(
    String opId,
    VaradhiTopicName topicFqn,
    TransitionType transitionType,
    String hostname,
    TransitionStage stage,
    String errorMsg
) {

    /** Whether this ack represents success — derived solely from {@link #errorMsg()}. */
    public boolean isSuccess() {
        return errorMsg == null || errorMsg.isEmpty();
    }

    /** Whether this ack represents failure — the inverse of {@link #isSuccess()}. */
    public boolean isFailure() {
        return !isSuccess();
    }

    public static TransitionAck success(
        String opId,
        VaradhiTopicName topicFqn,
        TransitionType transitionType,
        String hostname,
        TransitionStage stage
    ) {
        return new TransitionAck(opId, topicFqn, transitionType, hostname, stage, null);
    }

    public static TransitionAck failure(
        String opId,
        VaradhiTopicName topicFqn,
        TransitionType transitionType,
        String hostname,
        TransitionStage stage,
        String errorMsg
    ) {
        if (errorMsg == null || errorMsg.isEmpty()) {
            throw new IllegalArgumentException("failure ack requires a non-blank errorMsg");
        }
        return new TransitionAck(opId, topicFqn, transitionType, hostname, stage, errorMsg);
    }
}
