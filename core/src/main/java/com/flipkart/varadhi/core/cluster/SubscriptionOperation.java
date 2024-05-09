package com.flipkart.varadhi.core.cluster;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
public class SubscriptionOperation {
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
        op.setEndTime(0);
        op.data = new StopData(subscriptionId);
        return op;
    }

    @Override
    public String toString() {
        return String.format(
                "SubscriptionOperation{kind=%s, state=%s, requestedBy='%s', startTime=%d, endTime=%d, data=%s}", kind,
                state, requestedBy, startTime, endTime, data
        );
    }

    public enum Kind {
        START, STOP, UPDATE
    }

    public enum State {
        SCHEDULED, ERRORED, COMPLETED, IN_PROGRESS
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "@opDataType")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = StartData.class, name = "startSubData"),
            @JsonSubTypes.Type(value = StopData.class, name = "stopSubData"),
    })
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

        public boolean completed() {
            return state == State.COMPLETED || state == State.ERRORED;
        }

        @Override
        public String toString() {
            return String.format(
                    "OpData{subscriptionId='%s', operationId='%s', state=%s, errorMsg='%s'}", subscriptionId,
                    operationId, state, errorMsg
            );
        }
    }

    @Data
    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class StartData extends OpData {
        public StartData(String subscriptionId) {
            super(subscriptionId, UUID.randomUUID().toString(), State.SCHEDULED, null);
        }

        @Override
        public String toString() {
            return String.format("Start:%s", super.toString());
        }
    }

    @Data
    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class StopData extends OpData {
        public StopData(String subscriptionId) {
            super(subscriptionId, UUID.randomUUID().toString(), State.SCHEDULED, null);
        }

        @Override
        public String toString() {
            return String.format("Stop:%s", super.toString());
        }
    }
}
