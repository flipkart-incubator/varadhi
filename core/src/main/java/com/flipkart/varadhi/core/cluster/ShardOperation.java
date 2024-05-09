package com.flipkart.varadhi.core.cluster;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.flipkart.varadhi.entities.SubscriptionUnitShard;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
public class ShardOperation {
    private ShardOperation.Kind kind;
    private ShardOperation.State state;
    private String requestedBy;
    private long startTime;
    private long endTime;
    private ShardOperation.OpData data;

    public static ShardOperation startOp(
            int shardId, SubscriptionUnitShard shard, VaradhiSubscription subscription, String requestedBy
    ) {
        ShardOperation op = new ShardOperation();
        op.setKind(ShardOperation.Kind.START);
        op.setRequestedBy(requestedBy);
        op.setStartTime(System.currentTimeMillis());
        op.setEndTime(0);
        op.data = new ShardOperation.StartData(shardId, shard, subscription);
        return op;
    }

    @Override
    public String toString() {
        return String.format(
                "ShardOperation{kind=%s, state=%s, requestedBy='%s', startTime=%d, endTime=%d, data=%s}", kind, state,
                requestedBy, startTime, endTime, data
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
    @JsonSubTypes({@JsonSubTypes.Type(value = ShardOperation.StartData.class, name = "startShardData"),})
    public static class OpData {
        private int shardId;
        private VaradhiSubscription subscription;
        private SubscriptionUnitShard shard;
        private String operationId;
        private ShardOperation.State state;
        private String errorMsg;

        public void markFail(String reason) {
            state = ShardOperation.State.ERRORED;
            errorMsg = reason;
        }

        public void markInProgress() {
            state = ShardOperation.State.IN_PROGRESS;
        }

        @Override
        public String toString() {
            return String.format(
                    "OpData{subscriptionId='%s', shardId=%d, operationId='%s', state=%s, errorMsg='%s'}",
                    subscription.getName(), shard.getShardId(), operationId, state, errorMsg
            );
        }
    }

    @Data
    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class StartData extends ShardOperation.OpData {
        public StartData(int shardId, SubscriptionUnitShard shard, VaradhiSubscription subscription) {
            super(shardId, subscription, shard, UUID.randomUUID().toString(), ShardOperation.State.SCHEDULED, null);
        }

        @Override
        public String toString() {
            return String.format("Start:%s", super.toString());
        }
    }
}
