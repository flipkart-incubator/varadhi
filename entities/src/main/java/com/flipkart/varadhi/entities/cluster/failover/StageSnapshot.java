package com.flipkart.varadhi.entities.cluster.failover;

import java.util.List;

/**
 * Immutable per-stage history entry, appended when that stage ends.
 */
public record StageSnapshot(
    TransitionStage stage,
    long startedAt,
    List<String> ackedHosts,
    long endedAt,
    Outcome outcome,
    String errorMsg
) {
    public enum Outcome {
        OK, FAILED
    }

    public StageSnapshot {
        ackedHosts = ackedHosts == null ? List.of() : List.copyOf(ackedHosts);
    }

    static StageSnapshot completed(
        TransitionStage stage,
        long startedAt,
        List<String> ackedHosts,
        Outcome outcome,
        String errorMsg
    ) {
        return new StageSnapshot(stage, startedAt, ackedHosts, System.currentTimeMillis(), outcome, errorMsg);
    }
}
