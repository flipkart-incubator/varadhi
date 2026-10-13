package com.flipkart.varadhi.entities.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.flipkart.varadhi.entities.MetaStoreEntity;
import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.MetaStoreEntityType;
import com.flipkart.varadhi.entities.cluster.failover.StageSnapshot;
import com.flipkart.varadhi.entities.cluster.failover.TransitionStage;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.flipkart.varadhi.entities.cluster.Operation.State.COMPLETED;
import static com.flipkart.varadhi.entities.cluster.Operation.State.ERRORED;
import static com.flipkart.varadhi.entities.cluster.Operation.State.IN_PROGRESS;

/**
 * Durable intent + outcome record for a topic failover in the {@code OpStore}. Retained after the
 * ephemeral {@code TransitionMaster} is deleted. Carries per-stage {@link #stageHistory} for
 * audit/GET after COMPLETE/ABORT.
 *
 * <p>Ordering key is {@code "TopicFailover_" + topicFqn} so {@code OperationMgr} serializes all
 * failover work for a given topic. Retry limits come from the {@code RetryPolicy} passed when the
 * op is enqueued — not stamped on this record.
 */
@Getter
@EqualsAndHashCode (callSuper = true)
public class TopicFailoverOperation extends MetaStoreEntity implements OrderedOperation {

    private final String operationId;
    private final String topicFqn;
    private final String requestedBy;
    private final RegionName sourceRegion;
    private final RegionName targetRegion;
    private final boolean waitForReplicationLagToClear;
    private final int retryAttempt;
    private final long startTime;
    private long endTime;
    private State state;
    private String errorMsg;
    /** Per-stage audit log; survives past TransitionMaster deletion. */
    private final List<StageSnapshot> stageHistory;

    @JsonCreator
    TopicFailoverOperation(
        String operationId,
        int version,
        String topicFqn,
        String requestedBy,
        RegionName sourceRegion,
        RegionName targetRegion,
        boolean waitForReplicationLagToClear,
        int retryAttempt,
        long startTime,
        long endTime,
        State state,
        String errorMsg,
        List<StageSnapshot> stageHistory
    ) {
        super(operationId, version, MetaStoreEntityType.TOPIC_FAILOVER_OPERATION);
        this.operationId = operationId;
        this.topicFqn = topicFqn;
        this.requestedBy = requestedBy;
        this.sourceRegion = sourceRegion;
        this.targetRegion = targetRegion;
        this.waitForReplicationLagToClear = waitForReplicationLagToClear;
        this.retryAttempt = retryAttempt;
        this.startTime = startTime;
        this.endTime = endTime;
        this.state = state;
        this.errorMsg = errorMsg;
        this.stageHistory = stageHistory != null ? new ArrayList<>(stageHistory) : new ArrayList<>();
    }

    public static TopicFailoverOperation of(
        String topicFqn,
        RegionName sourceRegion,
        RegionName targetRegion,
        boolean waitForReplicationLagToClear,
        String requestedBy
    ) {
        return new TopicFailoverOperation(
            UUID.randomUUID().toString(),
            0,
            topicFqn,
            requestedBy,
            sourceRegion,
            targetRegion,
            waitForReplicationLagToClear,
            0,
            System.currentTimeMillis(),
            0,
            IN_PROGRESS,
            null,
            new ArrayList<>()
        );
    }

    /**
     * Starts {@code stage}, closing any open snapshot as {@link StageSnapshot.Outcome#OK}.
     */
    public void beginStage(TransitionStage stage) {
        completeOpenStage(StageSnapshot.Outcome.OK, null);
        stageHistory.add(StageSnapshot.started(stage));
    }

    private void completeOpenStage(StageSnapshot.Outcome outcome, String error) {
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
    }

    /**
     * Copies progress fields (state, error, endTime, stageHistory) from {@code src} onto this
     * store-loaded instance before persist — used by {@code OperationMgr} to avoid version races.
     */
    public void applyProgressFrom(TopicFailoverOperation src) {
        if (src == this) {
            return;
        }
        this.state = src.getState();
        this.errorMsg = src.getErrorMsg();
        this.endTime = src.getEndTime();
        this.stageHistory.clear();
        this.stageHistory.addAll(src.getStageHistory());
    }

    @JsonIgnore
    @Override
    public String getId() {
        return operationId;
    }

    @JsonIgnore
    @Override
    public String getOrderingKey() {
        return orderingKeyFor(topicFqn);
    }

    public static String orderingKeyFor(String topicFqn) {
        return "TopicFailover_" + topicFqn;
    }

    @Override
    public int getRetryAttempt() {
        return retryAttempt;
    }

    @Override
    public TopicFailoverOperation nextRetry() {
        return new TopicFailoverOperation(
            operationId,
            getVersion(),
            topicFqn,
            requestedBy,
            sourceRegion,
            targetRegion,
            waitForReplicationLagToClear,
            retryAttempt + 1,
            startTime,
            0,
            IN_PROGRESS,
            null,
            new ArrayList<>()
        );
    }

    @Override
    public State getState() {
        return state;
    }

    @Override
    public String getErrorMsg() {
        return errorMsg;
    }

    @JsonIgnore
    @Override
    public boolean isDone() {
        return state == COMPLETED || state == ERRORED;
    }

    @JsonIgnore
    @Override
    public boolean hasFailed() {
        return state == ERRORED;
    }

    @Override
    public void markFail(String error) {
        completeOpenStage(StageSnapshot.Outcome.FAILED, error);
        this.state = ERRORED;
        this.errorMsg = error;
        this.endTime = System.currentTimeMillis();
    }

    @Override
    public void markCompleted() {
        completeOpenStage(StageSnapshot.Outcome.OK, null);
        this.state = COMPLETED;
        this.endTime = System.currentTimeMillis();
    }

    public void update(State opState, String opError) {
        this.state = opState;
        this.errorMsg = opError;
        if (isDone()) {
            this.endTime = System.currentTimeMillis();
        }
    }

    @Override
    public String toString() {
        return String.format(
            "TopicFailoverOperation{opId=%s, topic=%s, %s->%s, state=%s, retry=%d, stages=%d}",
            operationId,
            topicFqn,
            sourceRegion,
            targetRegion,
            state,
            retryAttempt,
            stageHistory.size()
        );
    }
}
