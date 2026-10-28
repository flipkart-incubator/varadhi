package com.flipkart.varadhi.entities.cluster.transition;

import java.util.Map;

/**
 * Immutable per-stage history entry, appended when that stage ends.
 */
public record StageSnapshot(
    TransitionStage stage,
    long startedAt,
    Map<String, HostAck> hosts,
    long endedAt,
    Outcome outcome,
    String errorMsg
) {

    public StageSnapshot {
        hosts = Map.copyOf(hosts);
    }

    public enum Outcome {
        OK, FAILED
    }


    public enum HostAck {
        ACKED, UNACKED
    }

    public static StageSnapshot completed(
        TransitionStage stage,
        long startedAt,
        Map<String, HostAck> hosts,
        Outcome outcome,
        String errorMsg
    ) {
        return new StageSnapshot(stage, startedAt, hosts, System.currentTimeMillis(), outcome, errorMsg);
    }
}
