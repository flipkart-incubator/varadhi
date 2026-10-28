package com.flipkart.varadhi.entities.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.flipkart.varadhi.entities.MetaStoreEntity;
import com.flipkart.varadhi.entities.MetaStoreEntityType;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.UUID;

import static com.flipkart.varadhi.entities.cluster.Operation.State.COMPLETED;
import static com.flipkart.varadhi.entities.cluster.Operation.State.ERRORED;
import static com.flipkart.varadhi.entities.cluster.Operation.State.IN_PROGRESS;

/**
 * OperationMgr handle for one topic failover. Created in the same flow as its
 * {@code ActiveTransition}. Retained after that lock is deleted.
 *
 * <p>The id is the entity {@code name}. {@code topicFqn} is the ordering key and the lock
 * lookup key. {@link #requestedBy}, {@link #createdAt}, and {@link #updatedAt} are the op
 * audit fields.
 */
@Getter
@EqualsAndHashCode (callSuper = true)
public class TopicFailoverOperation extends MetaStoreEntity implements OrderedOperation {

    private final String topicFqn;
    private final String requestedBy;
    private final long createdAt;
    private final int retryAttempt;
    private long updatedAt;
    private State state;
    private String errorMsg;

    @JsonCreator
    TopicFailoverOperation(
        String name,
        int version,
        String topicFqn,
        String requestedBy,
        long createdAt,
        long updatedAt,
        int retryAttempt,
        State state,
        String errorMsg
    ) {
        super(name, version, MetaStoreEntityType.TOPIC_FAILOVER_OPERATION);
        this.topicFqn = topicFqn;
        this.requestedBy = requestedBy;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.retryAttempt = retryAttempt;
        this.state = state;
        this.errorMsg = errorMsg;
    }

    public static TopicFailoverOperation of(String topicFqn, String requestedBy) {
        long now = System.currentTimeMillis();
        return new TopicFailoverOperation(
            UUID.randomUUID().toString(),
            0,
            topicFqn,
            requestedBy,
            now,
            now,
            0,
            IN_PROGRESS,
            null
        );
    }

    /**
     * Copies progress fields (state, error) from {@code src} onto this store-loaded instance
     * before persist — used by {@code OperationMgr} to avoid version races.
     */
    public void applyProgressFrom(TopicFailoverOperation src) {
        if (src == this) {
            return;
        }
        this.state = src.getState();
        this.errorMsg = src.getErrorMsg();
        this.updatedAt = src.getUpdatedAt();
    }

    @JsonIgnore
    @Override
    public String getId() {
        return getName();
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
        long now = System.currentTimeMillis();
        return new TopicFailoverOperation(
            getName(),
            getVersion(),
            topicFqn,
            requestedBy,
            createdAt,
            now,
            retryAttempt + 1,
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
        this.updatedAt = System.currentTimeMillis();
    }

    @Override
    public void markCompleted() {
        this.state = COMPLETED;
        this.updatedAt = System.currentTimeMillis();
    }

    public void update(State opState, String opError) {
        this.state = opState;
        this.errorMsg = opError;
        this.updatedAt = System.currentTimeMillis();
    }

    @Override
    public String toString() {
        return String.format(
            "TopicFailoverOperation{opId=%s, topic=%s, state=%s, retry=%d}",
            getName(),
            topicFqn,
            state,
            retryAttempt
        );
    }
}
