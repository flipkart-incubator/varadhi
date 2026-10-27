package com.flipkart.varadhi.entities.cluster.failover;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.flipkart.varadhi.entities.RegionName;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

/**
 * Read-only snapshot of a {@link TransitionMaster} taken when a failover attempt ends.
 * Stored on {@code TopicFailoverOperation}; absent while the attempt is still running.
 */
@Getter
@EqualsAndHashCode
public class TransitionData {

    private final TransitionKind transitionKind;
    private final String operationId;
    private final String topicFqn;
    private final RegionName sourceRegion;
    private final RegionName targetRegion;
    private final boolean waitForReplicationLagToClear;
    private final String requestedBy;
    private final long createdAt;
    private final TransitionStage currentStage;
    private final long topicVersionToAwait;
    private final long updatedAt;
    private final List<StageSnapshot> stageHistory;

    @JsonCreator
    public TransitionData(
        TransitionKind transitionKind,
        String operationId,
        String topicFqn,
        RegionName sourceRegion,
        RegionName targetRegion,
        boolean waitForReplicationLagToClear,
        String requestedBy,
        long createdAt,
        TransitionStage currentStage,
        long topicVersionToAwait,
        long updatedAt,
        List<StageSnapshot> stageHistory
    ) {
        this.transitionKind = transitionKind;
        this.operationId = operationId;
        this.topicFqn = topicFqn;
        this.sourceRegion = sourceRegion;
        this.targetRegion = targetRegion;
        this.waitForReplicationLagToClear = waitForReplicationLagToClear;
        this.requestedBy = requestedBy;
        this.createdAt = createdAt;
        this.currentStage = currentStage;
        this.topicVersionToAwait = topicVersionToAwait;
        this.updatedAt = updatedAt;
        this.stageHistory = copyHistory(stageHistory);
    }

    public static TransitionData from(TransitionMaster master) {
        return new TransitionData(
            master.getTransitionKind(),
            master.getOperationId(),
            master.getTopicFqn(),
            master.getSourceRegion(),
            master.getTargetRegion(),
            master.isWaitForReplicationLagToClear(),
            master.getRequestedBy(),
            master.getCreatedAt(),
            master.getCurrentStage(),
            master.getTopicVersionToAwait(),
            master.getUpdatedAt(),
            master.getStageHistory()
        );
    }

    private static List<StageSnapshot> copyHistory(List<StageSnapshot> stageHistory) {
        if (stageHistory == null || stageHistory.isEmpty()) {
            return List.of();
        }
        List<StageSnapshot> copy = new ArrayList<>(stageHistory.size());
        for (StageSnapshot stage : stageHistory) {
            copy.add(
                new StageSnapshot(
                    stage.getStage(),
                    stage.getStartedAt(),
                    List.copyOf(stage.getAckedHosts()),
                    stage.getEndedAt(),
                    stage.getOutcome(),
                    stage.getErrorMsg()
                )
            );
        }
        return List.copyOf(copy);
    }
}
