package com.flipkart.varadhi.entities.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.flipkart.varadhi.entities.MetaStoreEntity;
import com.flipkart.varadhi.entities.MetaStoreEntityType;
import com.flipkart.varadhi.entities.cluster.failover.TransitionData;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.UUID;

import static com.flipkart.varadhi.entities.cluster.Operation.State.COMPLETED;
import static com.flipkart.varadhi.entities.cluster.Operation.State.ERRORED;
import static com.flipkart.varadhi.entities.cluster.Operation.State.IN_PROGRESS;

/**
 * OperationMgr handle for one topic failover. Created in the same flow as its
 * {@code TransitionMaster}. Retained after that master is deleted.
 *
 * <p>The id is the entity {@code name}. {@code topicFqn} is the ordering key and the master
 * lookup key. Request fields live on the master. {@link #transitionData} is null while the
 * attempt runs and is set from {@link TransitionData#from} before the terminal persist.
 * {@link #nextRetry()} leaves it null.
 */
@Getter
@EqualsAndHashCode (callSuper = true)
public class TopicFailoverOperation extends MetaStoreEntity implements OrderedOperation {

    private final String topicFqn;
    private final int retryAttempt;
    private State state;
    private String errorMsg;
    /** Null until the attempt ends, then a snapshot of the master. */
    private TransitionData transitionData;

    @JsonCreator
    TopicFailoverOperation(
        String name,
        int version,
        String topicFqn,
        int retryAttempt,
        State state,
        String errorMsg,
        TransitionData transitionData
    ) {
        super(name, version, MetaStoreEntityType.TOPIC_FAILOVER_OPERATION);
        this.topicFqn = topicFqn;
        this.retryAttempt = retryAttempt;
        this.state = state;
        this.errorMsg = errorMsg;
        this.transitionData = transitionData;
    }

    public static TopicFailoverOperation of(String topicFqn) {
        return new TopicFailoverOperation(UUID.randomUUID().toString(), 0, topicFqn, 0, IN_PROGRESS, null, null);
    }

    /** Terminal snapshot of the master. Set this before the final op persist. */
    public void attachTransitionData(TransitionData data) {
        if (data == null) {
            throw new IllegalArgumentException("transitionData is required");
        }
        this.transitionData = data;
    }

    /**
     * Copies progress fields (state, error, transitionData) from {@code src} onto this
     * store-loaded instance before persist — used by {@code OperationMgr} to avoid version races.
     */
    public void applyProgressFrom(TopicFailoverOperation src) {
        if (src == this) {
            return;
        }
        this.state = src.getState();
        this.errorMsg = src.getErrorMsg();
        this.transitionData = src.getTransitionData();
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
        return new TopicFailoverOperation(getName(), getVersion(), topicFqn, retryAttempt + 1, IN_PROGRESS, null, null);
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
    }

    @Override
    public void markCompleted() {
        this.state = COMPLETED;
    }

    public void update(State opState, String opError) {
        this.state = opState;
        this.errorMsg = opError;
    }

    @Override
    public String toString() {
        return String.format(
            "TopicFailoverOperation{opId=%s, topic=%s, state=%s, retry=%d, transitionData=%s}",
            getName(),
            topicFqn,
            state,
            retryAttempt,
            transitionData == null ? "absent" : "set"
        );
    }
}
