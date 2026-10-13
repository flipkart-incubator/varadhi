package com.flipkart.varadhi.entities.cluster.failover;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.flipkart.varadhi.entities.MetaStoreEntity;
import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.MetaStoreEntityType;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Ephemeral controller-only orchestration pointer for an in-flight topic transition.
 * Never broadcast / never in a pod cache — pods route only off {@code VaradhiTopic} version +
 * per-region {@code TopicState}.
 *
 * <p>{@code name == topicFqn}, so atomic ZK create enforces one active transition per topic.
 * Deleted at a terminal stage. Durable request/outcome and per-stage history live on
 * {@code TopicFailoverOperation} in the {@code OpStore}.
 */
@Getter
@EqualsAndHashCode (callSuper = true)
public class TransitionMaster extends MetaStoreEntity {

    private final TransitionKind transitionKind;
    private final String operationId;
    private final String topicFqn;
    private final RegionName sourceRegion;
    private final RegionName targetRegion;
    private final long createdAt;

    private TransitionStage currentStage;
    private long topicVersionToAwait;
    private long updatedAt;

    @JsonCreator
    TransitionMaster(
        String topicFqn,
        int version,
        TransitionKind transitionKind,
        String operationId,
        RegionName sourceRegion,
        RegionName targetRegion,
        long createdAt,
        TransitionStage currentStage,
        long topicVersionToAwait,
        long updatedAt
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
    }

    public static TransitionMaster forFailover(
        String operationId,
        String topicFqn,
        RegionName sourceRegion,
        RegionName targetRegion
    ) {
        long now = System.currentTimeMillis();
        return new TransitionMaster(
            topicFqn,
            0,
            TransitionKind.FAILOVER,
            operationId,
            sourceRegion,
            targetRegion,
            now,
            TransitionStage.PENDING,
            0L,
            now
        );
    }

    /**
     * Advances live stage + version-to-await. Per-stage history is recorded on the linked
     * {@code TopicFailoverOperation}, not here.
     */
    public void advanceTo(TransitionStage stage, long topicVersionToAwait) {
        this.currentStage = stage;
        this.topicVersionToAwait = topicVersionToAwait;
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
