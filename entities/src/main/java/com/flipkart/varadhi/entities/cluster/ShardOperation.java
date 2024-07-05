package com.flipkart.varadhi.entities.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.flipkart.varadhi.entities.*;
import lombok.*;

import java.util.UUID;

@Getter
@EqualsAndHashCode(callSuper = true)
public class ShardOperation extends MetaStoreEntity implements Operation {
    private final long startTime;
    private final OpData opData;
    private long endTime;

    @JsonCreator
    ShardOperation(String operationId, long startTime, long endTime, ShardOperation.OpData opData, int version) {
        super(operationId, version);
        this.startTime = startTime;
        this.endTime = endTime;
        this.opData = opData;
    }

    ShardOperation(ShardOperation.OpData opData) {
        super(opData.getOperationId(), 0);
        this.startTime = System.currentTimeMillis();
        this.endTime = 0;
        this.opData = opData;
    }

    public static ShardOperation startOp(
            String subOpId, SubscriptionUnitShard shard, VaradhiSubscription subscription
    ) {
        ShardOperation.OpData data = new ShardOperation.StartData(subOpId, shard, subscription);
        return new ShardOperation(data);
    }

    public static ShardOperation stopOp(String subOpId, SubscriptionUnitShard shard, VaradhiSubscription subscription) {
        ShardOperation.OpData data = new ShardOperation.StopData(subOpId, shard, subscription);
        return new ShardOperation(data);
    }

    @JsonIgnore
    @Override
    public String getId() {
        return opData.getOperationId();
    }

    @JsonIgnore
    @Override
    public boolean isDone() {
        return opData.isDone();
    }

    @Override
    public void markFail(String reason) {
        opData.markFail(reason);
        endTime = System.currentTimeMillis();
    }

    @Override
    public void markCompleted() {
        opData.markCompleted();
        endTime = System.currentTimeMillis();
    }

    public void update(ShardOperation.OpData updated) {
        opData.update(updated);
        if (opData.isDone()) {
            endTime = System.currentTimeMillis();
        }
    }


    public boolean hasFailed() {
        return opData.state == State.ERRORED;
    }

    @Override
    public String toString() {
        return String.format("{data=%s, startTime=%d, endTime=%d}", opData, startTime, endTime);
    }

    public enum State {
        ERRORED, COMPLETED, IN_PROGRESS
    }

    @Getter
    @Setter
    @AllArgsConstructor
    @NoArgsConstructor
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "@opDataType")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = ShardOperation.StartData.class, name = "startShardData"),
            @JsonSubTypes.Type(value = ShardOperation.StopData.class, name = "stopShardData"),
    })
    public static class OpData {
        private String operationId;
        private String parentOpId;

        private int shardId;
        private String subscriptionId;
        private String project;
        private boolean grouped;
        private Endpoint endpoint;
        private ConsumptionPolicy consumptionPolicy;
        private RetryPolicy retryPolicy;
        private SubscriptionUnitShard shard;

        private ShardOperation.State state;
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
                    "ParentOpId=%s Id='%s', subscriptionId='%s', shardId=%d, state=%s, errorMsg='%s'",
                    parentOpId, operationId, subscriptionId, shardId, state, errorMsg
            );
        }
    }


    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class StartData extends ShardOperation.OpData {
        StartData(String subOpId, SubscriptionUnitShard shard, VaradhiSubscription subscription) {
            super(
                    UUID.randomUUID().toString(), subOpId, shard.getShardId(), subscription.getName(),
                    subscription.getProject(), subscription.isGrouped(), subscription.getEndpoint(),
                    subscription.getConsumptionPolicy(), subscription.getRetryPolicy(), shard, State.IN_PROGRESS, null
            );
        }

        @Override
        public String toString() {
            return String.format("Start.OpData{%s}", super.toString());
        }
    }

    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class StopData extends ShardOperation.OpData {
        StopData(String subOpId, SubscriptionUnitShard shard, VaradhiSubscription subscription) {
            super(
                    UUID.randomUUID().toString(), subOpId, shard.getShardId(), subscription.getName(),
                    subscription.getProject(), subscription.isGrouped(), subscription.getEndpoint(),
                    subscription.getConsumptionPolicy(), subscription.getRetryPolicy(), shard, State.IN_PROGRESS, null
            );
        }

        @Override
        public String toString() {
            return String.format("Stop.OpData{%s}", super.toString());
        }
    }
}
