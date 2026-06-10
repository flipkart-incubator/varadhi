package com.flipkart.varadhi.entities.cluster.failover;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.ToString;

import java.util.Collections;
import java.util.List;

/**
 * Snapshot view returned by REST endpoints.  Combines the pointer (FTO) with the
 * mutable orchestration state stored on the underlying {@link TopicFailoverOperation}.
 */
@Getter
@ToString
public final class TopicFailoverTransition {
    private final String operationId;
    private final String topicFqn;
    private final String sourceRegion;
    private final String targetRegion;
    private final FailoverStage currentStage;
    private final long fenceVersion;
    private final long startTime;
    private final long endTime;
    private final String requestedBy;
    private final String errorMsg;
    private final List<StageSnapshot> stageHistory;

    @JsonCreator
    public TopicFailoverTransition(
        @JsonProperty ("operationId") String operationId,
        @JsonProperty ("topicFqn") String topicFqn,
        @JsonProperty ("sourceRegion") String sourceRegion,
        @JsonProperty ("targetRegion") String targetRegion,
        @JsonProperty ("currentStage") FailoverStage currentStage,
        @JsonProperty ("fenceVersion") long fenceVersion,
        @JsonProperty ("startTime") long startTime,
        @JsonProperty ("endTime") long endTime,
        @JsonProperty ("requestedBy") String requestedBy,
        @JsonProperty ("errorMsg") String errorMsg,
        @JsonProperty ("stageHistory") List<StageSnapshot> stageHistory
    ) {
        this.operationId = operationId;
        this.topicFqn = topicFqn;
        this.sourceRegion = sourceRegion;
        this.targetRegion = targetRegion;
        this.currentStage = currentStage;
        this.fenceVersion = fenceVersion;
        this.startTime = startTime;
        this.endTime = endTime;
        this.requestedBy = requestedBy;
        this.errorMsg = errorMsg;
        this.stageHistory = stageHistory == null ? Collections.emptyList() : List.copyOf(stageHistory);
    }

    public static TopicFailoverTransition from(TopicFailoverOperation op) {
        TopicFailoverOperation.OpData d = op.getData();
        return new TopicFailoverTransition(
            op.getId(),
            d.getTopicFqn(),
            d.getSourceRegion(),
            d.getTargetRegion(),
            op.getCurrentStage(),
            op.getFenceVersion(),
            op.getStartTime(),
            op.getEndTime(),
            op.getRequestedBy(),
            op.getErrorMsg(),
            op.getStageHistory()
        );
    }
}
