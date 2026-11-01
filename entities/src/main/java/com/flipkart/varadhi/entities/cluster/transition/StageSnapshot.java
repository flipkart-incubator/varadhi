package com.flipkart.varadhi.entities.cluster.transition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
        @JsonProperty ("ackedHosts") List<String> ackedHosts,
        @JsonProperty ("endedAt") long endedAt,
        @JsonProperty ("outcome") Outcome outcome,
        @JsonProperty ("errorMsg") String errorMsg
    ) {
        this.stage = stage;
        this.startedAt = startedAt;
        this.ackedHosts = ackedHosts != null ? ackedHosts : new ArrayList<>();
        this.hostAcks = this.ackedHosts.stream()
                                       .collect(java.util.stream.Collectors.toMap(host -> host, host -> HostAck.ACKED));
        this.endedAt = endedAt;
        this.outcome = outcome;
        this.errorMsg = errorMsg;
    }

    public StageSnapshot(
        TransitionStage stage,
        long startedAt,
        Map<String, HostAck> hosts,
        long endedAt,
        Outcome outcome,
        String errorMsg
    ) {
        this.stage = stage;
        this.startedAt = startedAt;
        this.ackedHosts = hosts.entrySet()
                               .stream()
                               .filter(entry -> entry.getValue() == HostAck.ACKED)
                               .map(Map.Entry::getKey)
                               .toList();
        this.hostAcks = Map.copyOf(hosts);
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
        return new StageSnapshot(stage, System.currentTimeMillis(), new ArrayList<>(), 0, Outcome.IN_PROGRESS, null);
    }
}
