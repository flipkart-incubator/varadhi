package com.flipkart.varadhi.entities.cluster.failover;

/**
 * Discriminates the wire payload carried by a {@link FailoverStageEvent}.
 *
 * <ul>
 *   <li>{@code PREPARE_HINT} – delivered for {@link FailoverStage#PREPARE}; pods
 *       should open / pre-warm target producer connections.</li>
 *   <li>{@code ACK_TRIGGER}  – delivered for {@link FailoverStage#SWITCH}; pods
 *       wait until their local topic cache observes the new tracked version
 *       and then ack.</li>
 *   <li>{@code TERMINAL}     – delivered for {@link FailoverStage#COMPLETED} /
 *       {@link FailoverStage#ABORTED} purely so pods can prune any in-memory
 *       per-op state; no ack required.</li>
 * </ul>
 */
public enum TriggerKind {
    PREPARE_HINT,
    ACK_TRIGGER,
    TERMINAL
}
