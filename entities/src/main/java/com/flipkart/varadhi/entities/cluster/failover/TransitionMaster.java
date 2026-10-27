package com.flipkart.varadhi.entities.cluster.failover;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.flipkart.varadhi.entities.MetaStoreEntity;
import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.MetaStoreEntityType;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

/**
 * Ephemeral controller-only orchestration state for an in-flight topic transition.
 * Never broadcast / never in a pod cache — pods route only off {@code VaradhiTopic} version +
 * per-region {@code TopicState}.
 *
 * <p>{@code name == topicFqn}, so atomic ZK create enforces one active transition per topic.
 * Holds the live stage and {@link #stageHistory}. Deleted once the attempt ends and that state
 * has been copied onto {@code TopicFailoverOperation#transitionData}.
 */
@Getter
@EqualsAndHashCode (callSuper = true)
public class TransitionMaster extends MetaStoreEntity {

    private final TransitionKind transitionKind;
    private final String operationId;
    private final String topicFqn;
    private final RegionName sourceRegion;
    private final RegionName targetRegion;
    private final boolean waitForReplicationLagToClear;
    private final String requestedBy;
    private final long createdAt;

    private TransitionStage currentStage;
    private long topicVersionToAwait;
    private long updatedAt;
    private final List<StageSnapshot> stageHistory;

    @JsonCreator
    TransitionMaster(
        String topicFqn,
        int version,
        TransitionKind transitionKind,
        String operationId,
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
        super(topicFqn, version, MetaStoreEntityType.TRANSITION_OBJECT);
        this.topicFqn = topicFqn;
        this.transitionKind = transitionKind;
        this.operationId = operationId;
        this.sourceRegion = sourceRegion;
        this.targetRegion = targetRegion;
        this.waitForReplicationLagToClear = waitForReplicationLagToClear;
        this.requestedBy = requestedBy;
        this.createdAt = createdAt;
        this.currentStage = currentStage;
        this.topicVersionToAwait = topicVersionToAwait;
        this.updatedAt = updatedAt;
        this.stageHistory = stageHistory != null ? new ArrayList<>(stageHistory) : new ArrayList<>();
    }

    public static TransitionMaster forFailover(
        String operationId,
        String topicFqn,
        RegionName sourceRegion,
        RegionName targetRegion,
        boolean waitForReplicationLagToClear,
        String requestedBy
    ) {
        long now = System.currentTimeMillis();
        return new TransitionMaster(
            topicFqn,
            0,
            TransitionKind.FAILOVER,
            operationId,
            sourceRegion,
            targetRegion,
            waitForReplicationLagToClear,
            requestedBy,
            now,
            TransitionStage.PENDING,
            0L,
            now,
            new ArrayList<>()
        );
    }

    /** Advances live stage + version-to-await. Stage history is recorded via {@link #beginStage}. */
    public void advanceTo(TransitionStage stage, long topicVersionToAwait) {
        this.currentStage = stage;
        this.topicVersionToAwait = topicVersionToAwait;
        this.updatedAt = System.currentTimeMillis();
    }

    /** Starts {@code stage}, closing any open snapshot as {@link StageSnapshot.Outcome#OK}. */
    public void beginStage(TransitionStage stage) {
        closeOpenStage(StageSnapshot.Outcome.OK, null);
        stageHistory.add(StageSnapshot.started(stage));
        this.updatedAt = System.currentTimeMillis();
    }

    /** Closes the in-progress snapshot, if any. Call before {@link TransitionData#from} on a terminal outcome. */
    public void closeOpenStage(StageSnapshot.Outcome outcome, String error) {
        if (stageHistory.isEmpty()) {
            return;
        }
        StageSnapshot last = stageHistory.get(stageHistory.size() - 1);
        if (last.getOutcome() != StageSnapshot.Outcome.IN_PROGRESS) {
            return;
        }
        last.setEndedAt(System.currentTimeMillis());
        last.setOutcome(outcome);
        last.setErrorMsg(error);
        this.updatedAt = System.currentTimeMillis();
    }

    @JsonIgnore
    public boolean isAbortable() {
        return currentStage.isAbortable();
    }

    @Override
    public String toString() {
        return String.format(
            "TransitionMaster{topic=%s, kind=%s, opId=%s, %s->%s, stage=%s, vToAwait=%d}",
            topicFqn,
            transitionKind,
            operationId,
            sourceRegion,
            targetRegion,
            currentStage,
            topicVersionToAwait
        );
    }
}
