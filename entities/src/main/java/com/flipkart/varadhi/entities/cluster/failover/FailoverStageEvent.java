package com.flipkart.varadhi.entities.cluster.failover;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.ToString;

import java.util.Objects;

/**
 * Immutable wire payload broadcast by the controller (and replayed point-to-point on
 * timeout / late-join) to every pod for one stage transition of a failover operation.
 *
 * <p>The same shape carries three distinct semantics, discriminated by {@link #kind}:
 * <ul>
 *   <li>{@link TriggerKind#PREPARE_HINT} – pre-warm target producer; {@link #topicVersionToAwait}
 *       is {@code -1}.</li>
 *   <li>{@link TriggerKind#ACK_TRIGGER}  – wait until the local topic cache catches up
 *       to {@link #topicVersionToAwait} (set by the controller after the atomic
 *       Op+Topic multi-txn at SWITCH) and then ack.</li>
 *   <li>{@link TriggerKind#TERMINAL}     – purely informational; pods may drop any
 *       per-op state. {@link #topicVersionToAwait} is {@code -1}.</li>
 * </ul>
 */
@Getter
@ToString
public final class FailoverStageEvent {

    /** Op identifier; also doubles as ack correlation id. */
    private final String opId;

    /** Fully qualified name of the parent resource (Topic / Subscription FQN). */
    private final String parentFqn;

    /**
     * Type of the parent: {@code "topic"} or {@code "subscription"}.
     * Stored as String so older pods can ignore unknown values without dying.
     */
    private final String parentKind;

    /** Stage we are signalling. Drives pod-side action via {@link FailoverStage#requiresAck()}. */
    private final FailoverStage stage;

    /** Trigger discriminator – see {@link TriggerKind}. */
    private final TriggerKind kind;

    /**
     * Monotonically increasing fence per op; bumped each time a new stage transition is
     * broadcast. Pods MUST ignore events whose fence is older than the one they already
     * accepted for this op (protects against late-arriving replays after timeout).
     */
    private final long fenceVersion;

    /**
     * Topic snapshot version that the pod's local L1 cache must observe before the
     * pod is allowed to ack. Set only for {@link TriggerKind#ACK_TRIGGER} (SWITCH);
     * {@code -1} otherwise.
     */
    private final int topicVersionToAwait;

    @JsonCreator
    public FailoverStageEvent(
        @JsonProperty ("opId") String opId,
        @JsonProperty ("parentFqn") String parentFqn,
        @JsonProperty ("parentKind") String parentKind,
        @JsonProperty ("stage") FailoverStage stage,
        @JsonProperty ("kind") TriggerKind kind,
        @JsonProperty ("fenceVersion") long fenceVersion,
        @JsonProperty ("topicVersionToAwait") int topicVersionToAwait
    ) {
        this.opId = Objects.requireNonNull(opId, "opId");
        this.parentFqn = Objects.requireNonNull(parentFqn, "parentFqn");
        this.parentKind = Objects.requireNonNull(parentKind, "parentKind");
        this.stage = Objects.requireNonNull(stage, "stage");
        this.kind = Objects.requireNonNull(kind, "kind");
        this.fenceVersion = fenceVersion;
        this.topicVersionToAwait = topicVersionToAwait;
    }

    public static FailoverStageEvent forPrepare(String opId, String parentFqn, String parentKind, long fenceVersion) {
        return new FailoverStageEvent(
            opId,
            parentFqn,
            parentKind,
            FailoverStage.PREPARE,
            TriggerKind.PREPARE_HINT,
            fenceVersion,
            -1
        );
    }

    public static FailoverStageEvent forSwitch(
        String opId,
        String parentFqn,
        String parentKind,
        long fenceVersion,
        int topicVersionToAwait
    ) {
        return new FailoverStageEvent(
            opId,
            parentFqn,
            parentKind,
            FailoverStage.SWITCH,
            TriggerKind.ACK_TRIGGER,
            fenceVersion,
            topicVersionToAwait
        );
    }

    public static FailoverStageEvent forTerminal(
        String opId,
        String parentFqn,
        String parentKind,
        FailoverStage terminal,
        long fenceVersion
    ) {
        if (!terminal.isTerminal()) {
            throw new IllegalArgumentException("forTerminal requires a terminal stage, got " + terminal);
        }
        return new FailoverStageEvent(opId, parentFqn, parentKind, terminal, TriggerKind.TERMINAL, fenceVersion, -1);
    }
}
