package com.flipkart.varadhi.entities.cluster.failover;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Immutable per-stage history record stored on {@link TopicFailoverOperation}.
 * Exists primarily for observability ({@code GET .../failover}) and post-mortem
 * inspection; the controller does not load StageSnapshots into in-memory state
 * (the in-memory barrier is the transient {@code StageAwaiter}).
 */
@Getter
@ToString
public final class StageSnapshot {
    private final FailoverStage stage;
    private final long fenceVersion;
    private final long startTime;
    private final long endTime;
    private final boolean success;
    private final String errorMsg;
    private final List<PodAckSnapshot> podAcks;

    @JsonCreator
    public StageSnapshot(
        @JsonProperty ("stage") FailoverStage stage,
        @JsonProperty ("fenceVersion") long fenceVersion,
        @JsonProperty ("startTime") long startTime,
        @JsonProperty ("endTime") long endTime,
        @JsonProperty ("success") boolean success,
        @JsonProperty ("errorMsg") String errorMsg,
        @JsonProperty ("podAcks") List<PodAckSnapshot> podAcks
    ) {
        this.stage = stage;
        this.fenceVersion = fenceVersion;
        this.startTime = startTime;
        this.endTime = endTime;
        this.success = success;
        this.errorMsg = errorMsg;
        this.podAcks = podAcks == null
            ? Collections.emptyList()
            : Collections.unmodifiableList(new ArrayList<>(podAcks));
    }

    public static StageSnapshot inProgress(FailoverStage stage, long fenceVersion) {
        return new StageSnapshot(stage, fenceVersion, System.currentTimeMillis(), 0L, false, null, List.of());
    }

    public StageSnapshot complete(boolean ok, String error, List<PodAckSnapshot> finalAcks) {
        return new StageSnapshot(
            stage,
            fenceVersion,
            startTime,
            System.currentTimeMillis(),
            ok,
            error,
            finalAcks
        );
    }
}
