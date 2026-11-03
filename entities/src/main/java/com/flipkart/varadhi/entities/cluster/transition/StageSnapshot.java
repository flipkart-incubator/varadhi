package com.flipkart.varadhi.entities.cluster.transition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

/**
 * Per-stage history entry on durable {@code TopicFailoverOperation}: when the stage started/ended,
 * which hosts acked (when populated), and the outcome. Survives past transition-operation
 * deletion so COMPLETE/ABORT still leave an audit trail.
 */
@Getter
public class StageSnapshot {

    private final TransitionStage stage;
    private final long startedAt;
    private final Map<String, HostAck> hostAcks;
    @Setter
    private long endedAt;
    @Setter
    private Outcome outcome;
    @Setter
    private String errorMsg;

    public enum Outcome {
        IN_PROGRESS, OK, FAILED
    }


    public enum HostAck {
        ACKED, UNACKED
    }

    @JsonCreator
    public StageSnapshot(
        @JsonProperty ("stage") TransitionStage stage,
        @JsonProperty ("startedAt") long startedAt,
        @JsonProperty ("hostAcks") Map<String, HostAck> hostAcks,
        @JsonProperty ("endedAt") long endedAt,
        @JsonProperty ("outcome") Outcome outcome,
        @JsonProperty ("errorMsg") String errorMsg
    ) {
        this.stage = stage;
        this.startedAt = startedAt;
        this.hostAcks = hostAcks != null ? Map.copyOf(hostAcks) : Map.of();
        this.endedAt = endedAt;
        this.outcome = outcome;
        this.errorMsg = errorMsg;
    }

    public TransitionStage stage() {
        return stage;
    }

    public Map<String, HostAck> hosts() {
        return hostAcks;
    }

    public Outcome outcome() {
        return outcome;
    }

    public String errorMsg() {
        return errorMsg;
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

    public static StageSnapshot started(TransitionStage stage) {
        return new StageSnapshot(stage, System.currentTimeMillis(), Map.of(), 0, Outcome.IN_PROGRESS, null);
    }
}
