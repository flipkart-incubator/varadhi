package com.flipkart.varadhi.entities.cluster.failover;

import com.fasterxml.jackson.annotation.JsonCreator;
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
 * Holds the live stage and completed {@link #stageHistory}. Deleted once the attempt ends.
 */
@Getter
@EqualsAndHashCode (callSuper = true)
public class ActiveTransition extends MetaStoreEntity {

    private final TransitionKind transitionKind;
    private final String operationId;
    private final String topicFqn;
    private final RegionName sourceRegion;
    private final RegionName targetRegion;
    private final boolean waitForReplicationLagToClear;

    private TransitionStage currentStage;
    private long topicVersionToAwait;
    private long stageStartedAt;
    private final List<StageSnapshot> stageHistory;

    @JsonCreator
    ActiveTransition(
        String topicFqn,
        int version,
        TransitionKind transitionKind,
        String operationId,
        RegionName sourceRegion,
        RegionName targetRegion,
        boolean waitForReplicationLagToClear,
        TransitionStage currentStage,
        long topicVersionToAwait,
        long stageStartedAt,
        List<StageSnapshot> stageHistory
    ) {
        super(topicFqn, version, MetaStoreEntityType.TRANSITION_OBJECT);
        this.topicFqn = topicFqn;
        this.transitionKind = transitionKind;
        this.operationId = operationId;
        this.sourceRegion = sourceRegion;
        this.targetRegion = targetRegion;
        this.waitForReplicationLagToClear = waitForReplicationLagToClear;
        this.currentStage = currentStage;
        this.topicVersionToAwait = topicVersionToAwait;
        this.stageStartedAt = stageStartedAt;
        this.stageHistory = stageHistory != null ? new ArrayList<>(stageHistory) : new ArrayList<>();
    }

    public static ActiveTransition forFailover(
        String operationId,
        String topicFqn,
        RegionName sourceRegion,
        RegionName targetRegion,
        boolean waitForReplicationLagToClear
    ) {
        return new ActiveTransition(
            topicFqn,
            0,
            TransitionKind.FAILOVER,
            operationId,
            sourceRegion,
            targetRegion,
            waitForReplicationLagToClear,
            TransitionStage.PENDING,
            0L,
            System.currentTimeMillis(),
            new ArrayList<>()
        );
    }

    /**
     * Records the stage being left, then starts {@code stage}. {@code error == null} means
     * {@link StageSnapshot.Outcome#OK}; otherwise {@link StageSnapshot.Outcome#FAILED}.
     */
    public void advanceTo(TransitionStage stage, long topicVersionToAwait, List<String> ackedHosts, String error) {
        StageSnapshot.Outcome outcome = error == null ? StageSnapshot.Outcome.OK : StageSnapshot.Outcome.FAILED;
        stageHistory.add(StageSnapshot.completed(currentStage, stageStartedAt, ackedHosts, outcome, error));
        this.currentStage = stage;
        this.topicVersionToAwait = topicVersionToAwait;
        this.stageStartedAt = System.currentTimeMillis();
    }

    @Override
    public String toString() {
        return String.format(
            "ActiveTransition{topic=%s, kind=%s, opId=%s, %s->%s, stage=%s, vToAwait=%d}",
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
