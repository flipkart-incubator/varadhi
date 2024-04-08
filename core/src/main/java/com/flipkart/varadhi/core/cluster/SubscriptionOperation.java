package com.flipkart.varadhi.core.cluster;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.UUID;

@Data
public class SubscriptionOperation {
    public enum Kind {
        START, STOP, UPDATE
    }
    public enum State {
        SCHEDULED, ERRORED, COMPLETED, IN_PROGRESS
    }
    private Kind kind;
    private State state;
    private String requestedBy;
    private long startTime;
    private long endTime;
    private OpData data;
    public static SubscriptionOperation startOp(String subscriptionId, String requestedBy) {
        SubscriptionOperation op = new SubscriptionOperation();
        op.setKind(Kind.START);
        op.setRequestedBy(requestedBy);
        op.setStartTime(System.currentTimeMillis());
        op.data = new StartData(subscriptionId);
        return op;
    }

    public static SubscriptionOperation stopOp(String subscriptionId, String requestedBy) {
        SubscriptionOperation op = new SubscriptionOperation();
        op.setKind(Kind.STOP);
        op.setRequestedBy(requestedBy);
        op.setStartTime(System.currentTimeMillis());
        op.data = new StopData(subscriptionId);
        return op;
    }

    @Data
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME,  property = "@opDataType")
    @JsonSubTypes({@JsonSubTypes.Type(value = StartData.class, name = "startData"),})
    public static class OpData {
        private String subscriptionId;
        private String operationId;
        private State state;
        private String errorMsg;

        public void markFail(String reason) {
            state = State.ERRORED;
            errorMsg = reason;
        }
        public void markInProgress() {
            state = State.IN_PROGRESS;
        }
    }

    @Data
    @EqualsAndHashCode(callSuper = true)
    public static class StartData extends OpData {
        public StartData(String subscriptionId) {
            this.setOperationId(UUID.randomUUID().toString());
            this.setSubscriptionId(subscriptionId);
            this.setState(State.SCHEDULED);
        }
    }

    @Data
    @EqualsAndHashCode(callSuper = true)
    public static class StopData extends OpData {
        public StopData(String subscriptionId) {
            this.setOperationId(UUID.randomUUID().toString());
            this.setSubscriptionId(subscriptionId);
            this.setState(State.SCHEDULED);
        }
    }
}
