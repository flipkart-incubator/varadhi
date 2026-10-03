package com.flipkart.varadhi.entities.cluster.failover;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

/**
 * Per-stage history entry on durable {@code TopicFailoverOperation}: when the stage started/ended,
 * which hosts acked (when populated), and the outcome. Survives past {@code TransitionMaster}
 * deletion so COMPLETE/ABORT still leave an audit trail.
 */
@Getter
public class StageSnapshot {

    private final TransitionStage stage;
    private final long startedAt;
    private final List<String> ackedHosts;
    @Setter
    private long endedAt;
    @Setter
    private Outcome outcome;
    @Setter
    private String errorMsg;

    public enum Outcome {
        IN_PROGRESS, OK, FAILED
    }

    @JsonCreator
    public StageSnapshot(
        @JsonProperty ("stage") TransitionStage stage,
        @JsonProperty ("startedAt") long startedAt,
        @JsonProperty ("ackedHosts") List<String> ackedHosts,
        @JsonProperty ("endedAt") long endedAt,
        @JsonProperty ("outcome") Outcome outcome,
        @JsonProperty ("errorMsg") String errorMsg
    ) {
        this.stage = stage;
        this.startedAt = startedAt;
        this.ackedHosts = ackedHosts != null ? ackedHosts : new ArrayList<>();
        this.endedAt = endedAt;
        this.outcome = outcome;
        this.errorMsg = errorMsg;
    }

    public static StageSnapshot started(TransitionStage stage) {
        return new StageSnapshot(stage, System.currentTimeMillis(), new ArrayList<>(), 0, Outcome.IN_PROGRESS, null);
    }
}
