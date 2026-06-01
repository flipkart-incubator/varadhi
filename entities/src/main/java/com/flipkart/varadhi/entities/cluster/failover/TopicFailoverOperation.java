package com.flipkart.varadhi.entities.cluster.failover;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.varadhi.entities.MetaStoreEntity;
import com.flipkart.varadhi.entities.MetaStoreEntityType;
import com.flipkart.varadhi.entities.cluster.OrderedOperation;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Single-source-of-truth orchestration entity for a topic failover.
 *
 * <p>Unlike {@code SubscriptionOperation} this op has no child shard ops: it is a
 * linear staged workflow driven by {@code TopicFailoverOpExecutor}. The
 * controller advances {@link #currentStage} at each transition and (for stages
 * that require it) bumps {@link #fenceVersion}; both fields are persisted under
 * the same Op znode via {@code OpStore.updateTopicFailoverOp}.
 *
 * <p>Pods read nothing from this entity directly; everything they need is on
 * the {@link FailoverStageEvent} payload that the controller broadcasts.
 */
@Getter
@EqualsAndHashCode (callSuper = true)
public class TopicFailoverOperation extends MetaStoreEntity implements OrderedOperation {

    private final String requestedBy;
    private final long startTime;
    private final OpData data;
    private final int retryAttempt;
    private final List<StageSnapshot> stageHistory;
    private FailoverStage currentStage;
    private long fenceVersion;
    private long endTime;
    private State opState;
    private String errorMsg;

    @JsonCreator
    public TopicFailoverOperation(
        @JsonProperty ("name") String operationId,
        @JsonProperty ("version") int version,
        @JsonProperty ("requestedBy") String requestedBy,
        @JsonProperty ("startTime") long startTime,
        @JsonProperty ("endTime") long endTime,
        @JsonProperty ("data") OpData data,
        @JsonProperty ("retryAttempt") int retryAttempt,
        @JsonProperty ("currentStage") FailoverStage currentStage,
        @JsonProperty ("fenceVersion") long fenceVersion,
        @JsonProperty ("stageHistory") List<StageSnapshot> stageHistory,
        @JsonProperty ("opState") State opState,
        @JsonProperty ("errorMsg") String errorMsg
    ) {
        super(operationId, version, MetaStoreEntityType.TOPIC_FAILOVER_OPERATION);
        this.requestedBy = requestedBy;
        this.startTime = startTime;
        this.endTime = endTime;
        this.data = Objects.requireNonNull(data, "data");
        this.retryAttempt = retryAttempt;
        this.currentStage = currentStage == null ? FailoverStage.PENDING : currentStage;
        this.fenceVersion = fenceVersion;
        this.stageHistory = stageHistory == null ? new ArrayList<>() : new ArrayList<>(stageHistory);
        this.opState = opState == null ? State.IN_PROGRESS : opState;
        this.errorMsg = errorMsg;
    }

    private TopicFailoverOperation(OpData data, String requestedBy) {
        super(data.getOperationId(), 0, MetaStoreEntityType.TOPIC_FAILOVER_OPERATION);
        this.requestedBy = requestedBy;
        this.startTime = System.currentTimeMillis();
        this.endTime = 0L;
        this.data = data;
        this.retryAttempt = 0;
        this.currentStage = FailoverStage.PENDING;
        this.fenceVersion = 0L;
        this.stageHistory = new ArrayList<>();
        this.opState = State.IN_PROGRESS;
        this.errorMsg = null;
    }

    public static TopicFailoverOperation create(
        String topicFqn,
        String sourceRegion,
        String targetRegion,
        boolean waitForReplicationLagToClear,
        boolean skipValidation,
        String requestedBy
    ) {
        OpData data = new OpData(
            UUID.randomUUID().toString(),
            topicFqn,
            sourceRegion,
            targetRegion,
            waitForReplicationLagToClear,
            skipValidation
        );
        return new TopicFailoverOperation(data, requestedBy);
    }

    /**
     * Advance to a new stage and (for stages requiring an ack barrier) bump the fence.
     *
     * <p>Returns a fresh {@link StageSnapshot} which the caller should treat as the
     * "in progress" record for this stage; once acks land (or timeout fires) the caller
     * supplies the completed snapshot via {@link #completeStage(StageSnapshot)}.
     */
    public StageSnapshot advanceStage(FailoverStage next) {
        this.currentStage = Objects.requireNonNull(next, "next");
        if (next.requiresAck()) {
            this.fenceVersion += 1;
        }
        return StageSnapshot.inProgress(next, this.fenceVersion);
    }

    public void completeStage(StageSnapshot finalSnapshot) {
        Objects.requireNonNull(finalSnapshot, "finalSnapshot");
        this.stageHistory.add(finalSnapshot);
    }

    @JsonIgnore
    @Override
    public String getId() {
        return data.getOperationId();
    }

    @JsonIgnore
    @Override
    public State getState() {
        return opState;
    }

    @JsonIgnore
    @Override
    public String getErrorMsg() {
        return errorMsg;
    }

    @JsonIgnore
    @Override
    public boolean isDone() {
        return opState == State.COMPLETED || opState == State.ERRORED;
    }

    @JsonIgnore
    @Override
    public boolean hasFailed() {
        return opState == State.ERRORED;
    }

    @Override
    public void markFail(String reason) {
        this.opState = State.ERRORED;
        this.errorMsg = reason;
        this.currentStage = FailoverStage.ABORTED;
        this.endTime = System.currentTimeMillis();
    }

    @Override
    public void markCompleted() {
        this.opState = State.COMPLETED;
        this.currentStage = FailoverStage.COMPLETED;
        this.endTime = System.currentTimeMillis();
    }

    @JsonIgnore
    @Override
    public String getOrderingKey() {
        return "TopicFailover_" + data.getTopicFqn();
    }

    @Override
    public TopicFailoverOperation nextRetry() {
        return new TopicFailoverOperation(
            getId(),
            getVersion(),
            requestedBy,
            startTime,
            0L,
            data,
            retryAttempt + 1,
            FailoverStage.PENDING,
            fenceVersion,
            stageHistory,
            State.IN_PROGRESS,
            null
        );
    }

    @Override
    public String toString() {
        return "TopicFailoverOperation{id=%s, topic=%s, stage=%s, fence=%d, state=%s}".formatted(
            getId(),
            data.getTopicFqn(),
            currentStage,
            fenceVersion,
            opState
        );
    }

    /**
     * Immutable request payload describing the failover the user asked for.
     */
    @Getter
    public static final class OpData {
        private final String operationId;
        private final String topicFqn;
        private final String sourceRegion;
        private final String targetRegion;
        private final boolean waitForReplicationLagToClear;
        private final boolean skipValidation;

        @JsonCreator
        public OpData(
            @JsonProperty ("operationId") String operationId,
            @JsonProperty ("topicFqn") String topicFqn,
            @JsonProperty ("sourceRegion") String sourceRegion,
            @JsonProperty ("targetRegion") String targetRegion,
            @JsonProperty ("waitForReplicationLagToClear") boolean waitForReplicationLagToClear,
            @JsonProperty ("skipValidation") boolean skipValidation
        ) {
            this.operationId = Objects.requireNonNull(operationId, "operationId");
            this.topicFqn = Objects.requireNonNull(topicFqn, "topicFqn");
            this.sourceRegion = Objects.requireNonNull(sourceRegion, "sourceRegion");
            this.targetRegion = Objects.requireNonNull(targetRegion, "targetRegion");
            this.waitForReplicationLagToClear = waitForReplicationLagToClear;
            this.skipValidation = skipValidation;
        }
    }
}
