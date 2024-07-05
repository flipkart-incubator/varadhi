package com.flipkart.varadhi.entities.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.flipkart.varadhi.entities.MetaStoreEntity;
import lombok.*;

import java.util.List;
import java.util.UUID;

@Getter
@EqualsAndHashCode(callSuper = true)
public class SubscriptionOperation extends MetaStoreEntity implements OrderedOperation {
    private final String requestedBy;
    private final long startTime;
    private final OpData data;
    private long endTime;

    @JsonCreator
    SubscriptionOperation(
            String operationId, String requestedBy, long startTime, long endTime, OpData data, int version
    ) {
        super(operationId, version);
        this.requestedBy = requestedBy;
        this.startTime = startTime;
        this.endTime = endTime;
        this.data = data;
    }

    SubscriptionOperation(OpData data, String requestedBy) {
        super(data.operationId, 0);
        this.requestedBy = requestedBy;
        this.startTime = System.currentTimeMillis();
        this.endTime = 0;
        this.data = data;
    }

    public static SubscriptionOperation startOp(String subscriptionId, String requestedBy) {
        OpData data = new StartData(subscriptionId);
        return new SubscriptionOperation(data, requestedBy);
    }

    public static SubscriptionOperation stopOp(String subscriptionId, String requestedBy) {
        OpData data = new StopData(subscriptionId);
        return new SubscriptionOperation(data, requestedBy);
    }

    public static SubscriptionOperation reAssignShardOp(Assignment assignment, String requestedBy) {
        OpData data = new ReassignShardData(assignment);
        return new SubscriptionOperation(data, requestedBy);
    }

    @JsonIgnore
    @Override
    public String getId() {
        return data.getOperationId();
    }

    @JsonIgnore
    @Override
    public String getOrderingKey() {
        return "Sub_" + data.getSubscriptionId();
    }

    @JsonIgnore
    @Override
    public boolean isDone() {
        return data.state == State.COMPLETED || data.state == State.ERRORED;
    }


    @Override
    public void markFail(String reason) {
        data.markFail(reason);
        endTime = System.currentTimeMillis();
    }

    @Override
    public void markCompleted() {
        data.markCompleted();
        endTime = System.currentTimeMillis();
    }

    public void update(SubscriptionOperation updated) {
        data.update(updated.data);
        if (data.isDone()) {
            endTime = System.currentTimeMillis();
        }
    }

    public void update(List<ShardOperation> shardOps) {
        // This assumes that caller passes the complete list of shardOps for the respective SubOp.
        StringBuilder sb = new StringBuilder();
        int completedCount = 0;
        for (ShardOperation shardOp : shardOps) {
            ShardOperation.OpData opData = shardOp.getOpData();
            if (shardOp.hasFailed()) {
                if (!sb.isEmpty()) {
                    sb.append(", ");
                }
                sb.append(String.format("Shard:%d failed:%s", opData.getShardId(), opData.getErrorMsg()));
            }
            if (shardOp.isDone()) {
                completedCount++;
            }
        }

        if (completedCount == shardOps.size()) {
            if (sb.isEmpty()) {
                markCompleted();
            } else {
                markFail(sb.toString());
            }
        }
    }

    @Override
    public String toString() {
        return String.format(
                "{data=%s requestedBy='%s', startTime=%d, endTime=%d}", data, requestedBy,
                startTime, endTime
        );
    }

    public enum State {
        ERRORED, COMPLETED, IN_PROGRESS
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "@opDataType")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = StartData.class, name = "startSubData"),
            @JsonSubTypes.Type(value = StopData.class, name = "stopSubData"),
            @JsonSubTypes.Type(value = ReassignShardData.class, name = "reassignShardData"),
    })
    public static class OpData {
        private String operationId;
        private String subscriptionId;
        private State state;
        private String errorMsg;

        public void markFail(String reason) {
            state = State.ERRORED;
            errorMsg = reason;
        }

        public void markCompleted() {
            state = State.COMPLETED;
        }

        @JsonIgnore
        public boolean isDone() {
            return state == State.ERRORED || state == State.COMPLETED;
        }

        public void update(OpData updated) {
            if (!operationId.equals(updated.operationId)) {
                throw new IllegalArgumentException("Update failed. Operation Id mismatch.");
            }
            errorMsg = updated.errorMsg;
            state = updated.state;
        }

        @Override
        public String toString() {
            return String.format(
                    "Id=%s, subscriptionId='%s', state=%s, errorMsg='%s'", operationId, subscriptionId, state,
                    errorMsg
            );
        }
    }

    @Data
    @NoArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class StartData extends OpData {
        StartData(String subscriptionId) {
            super(UUID.randomUUID().toString(), subscriptionId, State.IN_PROGRESS, null);
        }

        @Override
        public String toString() {
            return String.format("Start.OpData{%s}", super.toString());
        }
    }

    @Data
    @NoArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class StopData extends OpData {
        StopData(String subscriptionId) {
            super(UUID.randomUUID().toString(), subscriptionId, State.IN_PROGRESS, null);
        }

        @Override
        public String toString() {
            return String.format("Stop.OpData{%s}", super.toString());
        }
    }

    @Data
    @NoArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class ReassignShardData extends OpData {
        private Assignment assignment;

        ReassignShardData(Assignment assignment) {
            super(UUID.randomUUID().toString(), assignment.getSubscriptionId(), State.IN_PROGRESS, null);
            this.assignment = assignment;
        }

        @Override
        public String toString() {
            return String.format("ReassignShard.OpData{%s %s}", super.toString(), assignment);
        }
    }
}
