package com.flipkart.varadhi.entities.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.flipkart.varadhi.entities.MetaStoreEntity;
import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.MetaStoreEntityType;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.UUID;

import static com.flipkart.varadhi.entities.cluster.Operation.State.COMPLETED;
import static com.flipkart.varadhi.entities.cluster.Operation.State.ERRORED;
import static com.flipkart.varadhi.entities.cluster.Operation.State.IN_PROGRESS;

/**
 * The <b>intent</b> record for a topic failover, persisted in the {@code OpStore} and retained
 * forever as history. It captures <em>what was requested</em> and the final outcome; the live
 * per-stage orchestration state lives on the {@code TransitionObject} (the master).
 *
 * <p>Ordering key is {@code "TopicFailover_" + topicFqn} so the {@code OperationMgr} serializes
 * all failover work for a given topic.
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
        String errorMsg
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
            null
        );
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
            null
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
        this.state = ERRORED;
        this.errorMsg = error;
        this.endTime = System.currentTimeMillis();
    }

    @Override
    public void markCompleted() {
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
            "TopicFailoverOperation{opId=%s, topic=%s, %s->%s, state=%s, retry=%d}",
            operationId,
            topicFqn,
            sourceRegion,
            targetRegion,
            state,
            retryAttempt
        );
    }
}
