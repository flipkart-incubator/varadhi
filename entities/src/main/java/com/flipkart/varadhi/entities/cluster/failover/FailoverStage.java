package com.flipkart.varadhi.entities.cluster.failover;

/**
 * Lifecycle of a topic (or subscription) failover operation.
 *
 * <p>State machine:
 * <pre>
 *   PENDING -> PREPARE -> SWITCH -> DRAIN -> COMPLETED
 *                |          |        |
 *                +----------+--------+----> ABORTED
 * </pre>
 *
 * <ul>
 *   <li>{@code PENDING}    – Op persisted; not yet picked up by the executor.</li>
 *   <li>{@code PREPARE}    – Controller broadcasts a "warm the target" hint;
 *                            pods open producer connections and lazily initialise
 *                            metadata. Requires per-pod ack barrier.</li>
 *   <li>{@code SWITCH}     – Atomically rewrites the {@link com.flipkart.varadhi.entities.VaradhiTopic}
 *                            snapshot so that source.TopicState = Blocked,
 *                            target.TopicState = Producing. Pods ack only after
 *                            their local L1 cache observes the new topic version.</li>
 *   <li>{@code DRAIN}      – Pure controller-side wait for source replication lag
 *                            to clear. No pod involvement, no broadcast.</li>
 *   <li>{@code COMPLETED}  – Source flipped to Replicating (untracked) and FTO deleted.</li>
 *   <li>{@code ABORTED}    – Allowed before SWITCH succeeds; cleans up FTO and
 *                            leaves the topic snapshot untouched.</li>
 * </ul>
 */
public enum FailoverStage {
    PENDING(false, false),
    PREPARE(true, true),
    SWITCH(true, true),
    DRAIN(false, false),
    COMPLETED(false, false),
    ABORTED(false, false);

    private final boolean abortable;
    private final boolean requiresAck;

    FailoverStage(boolean abortable, boolean requiresAck) {
        this.abortable = abortable;
        this.requiresAck = requiresAck;
    }

    /** Abort is rejected once we are past SWITCH (i.e. DRAIN / COMPLETED / ABORTED). */
    public boolean isAbortable() {
        return abortable;
    }

    /** Whether the controller must wait for pod acks before advancing past this stage. */
    public boolean requiresAck() {
        return requiresAck;
    }

    public boolean isTerminal() {
        return this == COMPLETED || this == ABORTED;
    }
}
