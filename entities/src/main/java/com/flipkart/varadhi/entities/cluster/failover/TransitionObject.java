package com.flipkart.varadhi.entities.cluster.failover;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.flipkart.varadhi.entities.MetaStoreEntity;
import com.flipkart.varadhi.entities.MetaStoreEntityType;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

/**
 * The <b>master</b> orchestration state for an in-flight topic transition. Controller-only:
 * it is never broadcast and never present in any pod cache, so no pod-side decision may depend
 * on it (pods route only off the {@code VaradhiTopic} version + per-region {@code TopicState}).
 *
 * <p>{@code name == topicFqn}, so an atomic ZK create enforces "one active transition per topic"
 * (the lock-free uniqueness guard). It is deleted when the transition reaches a terminal stage.
 */
@Getter
@EqualsAndHashCode (callSuper = true)
public class TransitionObject extends MetaStoreEntity {

    private final TransitionKind transitionKind;
    private final String operationId;
    private final String topicFqn;
    private final String sourceRegion;
    private final String targetRegion;
    private final long createdAt;

    private TransitionStage currentStage;
    private long topicVersionToAwait;
    private long updatedAt;
    private final List<StageSnapshot> stageHistory;

    @JsonCreator
    TransitionObject(
        String topicFqn,
        int version,
        TransitionKind transitionKind,
        String operationId,
        String sourceRegion,
        String targetRegion,
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
        this.createdAt = createdAt;
        this.currentStage = currentStage;
        this.topicVersionToAwait = topicVersionToAwait;
        this.updatedAt = updatedAt;
        this.stageHistory = stageHistory != null ? stageHistory : new ArrayList<>();
    }

    public static TransitionObject forFailover(
        String operationId,
        String topicFqn,
        String sourceRegion,
        String targetRegion
    ) {
        long now = System.currentTimeMillis();
        return new TransitionObject(
            topicFqn,
            0,
            TransitionKind.FAILOVER,
            operationId,
            sourceRegion,
            targetRegion,
            now,
            TransitionStage.PENDING,
            0L,
            now,
            new ArrayList<>()
        );
    }

    /**
     * Advances the master to {@code stage}, stamping {@code topicVersionToAwait} (0 when the
     * stage is not version-gated) and appending a new {@link StageSnapshot}.
     */
    public void advanceTo(TransitionStage stage, long topicVersionToAwait) {
        this.currentStage = stage;
        this.topicVersionToAwait = topicVersionToAwait;
        this.updatedAt = System.currentTimeMillis();
        this.stageHistory.add(StageSnapshot.started(stage));
    }

    @JsonIgnore
    public StageSnapshot currentStageSnapshot() {
        return stageHistory.isEmpty() ? null : stageHistory.get(stageHistory.size() - 1);
    }

    @JsonIgnore
    public boolean isAbortable() {
        return currentStage.isAbortable();
    }

    @Override
    public String toString() {
        return String.format(
            "TransitionObject{topic=%s, kind=%s, opId=%s, %s->%s, stage=%s, vToAwait=%d}",
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
