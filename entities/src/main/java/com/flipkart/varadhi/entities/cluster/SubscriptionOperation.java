package com.flipkart.varadhi.entities.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.flipkart.varadhi.entities.MetaStoreEntity;
import lombok.*;

import java.util.List;
import java.util.UUID;

@Getter
@EqualsAndHashCode(callSuper = true)
public class SubscriptionOperation extends MetaStoreEntity {
    private final String requestedBy;
    private final long startTime;
    private long endTime;
    private final OpData data;

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

    public void markFail(String reason) {
        data.state = State.ERRORED;
        data.errorMsg = reason;
        endTime = System.currentTimeMillis();
    }

    public void markCompleted() {
        data.state = State.COMPLETED;
        endTime = System.currentTimeMillis();
    }

    public void update(SubscriptionOperation updated) {
        if (!data.operationId.equals(updated.data.operationId)) {
            throw new IllegalArgumentException("Update failed. Operation Id mismatch.");
        }
        data.errorMsg = updated.data.errorMsg;
        data.state = updated.data.state;
        endTime = updated.endTime;
    }

    public void update(List<ShardOperation> shardOps) {
        StringBuilder sb = new StringBuilder();
        int completedCount = 0;
        for(ShardOperation shardOp : shardOps) {
            ShardOperation.OpData opData = shardOp.getOpData();
            if (shardOp.hasFailed()) {
                sb.append(String.format("Shard:%d failed:%s", opData.getShardId(), opData.getErrorMsg()));
            }
            if (shardOp.hasCompleted()) {
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


    public boolean completed() {
        return data.state == State.COMPLETED || data.state == State.ERRORED;
    }

    @Override
    public String toString() {
        return String.format(
                "{data=%s requestedBy='%s', startTime=%d, endTime=%d}", data, requestedBy,
                startTime, endTime
        );
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
        private String operationId;
        private String subscriptionId;
        private State state;
        private String errorMsg;
        @Override
        public String toString() {
            return String.format(
                    "OpData{Id=%s, subscriptionId='%s', state=%s, errorMsg='%s'}", operationId, subscriptionId, state,
                    errorMsg
            );
        }
    }

    @Data
    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class StartData extends OpData {
        StartData(String subscriptionId) {
            super(UUID.randomUUID().toString(), subscriptionId, State.IN_PROGRESS, null);
        }

        @Override
        public String toString() {
            return String.format("Start.%s", super.toString());
        }
    }

    @Data
    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class StopData extends OpData {
        StopData(String subscriptionId) {
            super(UUID.randomUUID().toString(), subscriptionId, State.SCHEDULED, null);
        }

        @Override
        public String toString() {
            return String.format("Stop.%s", super.toString());
        }
    }
}
