package com.flipkart.varadhi.entities.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.flipkart.varadhi.entities.*;
import lombok.*;

import java.util.UUID;

import static com.flipkart.varadhi.entities.cluster.Operation.State.*;

@Getter
@EqualsAndHashCode(callSuper = true)
public class ShardOperation extends MetaStoreEntity implements Operation {
    private final OpData opData;
    private long startTime;
    private long endTime;
    private State state;
    private String errorMsg;

    @JsonCreator
    ShardOperation(
            String operationId, int version, long startTime, long endTime, State state, String errorMsg,
            ShardOperation.OpData opData
    ) {
        super(operationId, version);
        this.state = state;
        this.errorMsg = errorMsg;
        this.startTime = startTime;
        this.endTime = endTime;
        this.opData = opData;
    }

    ShardOperation(ShardOperation.OpData opData) {
        super(opData.getOperationId(), 0);
        this.state = IN_PROGRESS;
        this.startTime = System.currentTimeMillis();
        this.endTime = 0;
        this.opData = opData;
    }

    public static ShardOperation startOp(
            String subOpId, SubscriptionUnitShard shard, VaradhiSubscription subscription
    ) {
        return new ShardOperation(new ShardOperation.StartData(subOpId, shard, subscription));
    }

    public static ShardOperation stopOp(String subOpId, SubscriptionUnitShard shard, VaradhiSubscription subscription) {
        return new ShardOperation(new ShardOperation.StopData(subOpId, shard, subscription));
    }

    @JsonIgnore
    @Override
    public String getId() {
        return opData.getOperationId();
    }

    @JsonIgnore
    @Override
    public boolean isDone() {
        return state == ERRORED || state == COMPLETED;
    }

    @Override
    public void markFail(String reason) {
        state = ERRORED;
        errorMsg = reason;
        endTime = System.currentTimeMillis();
    }

    @Override
    public void markCompleted() {
        state = COMPLETED;
        endTime = System.currentTimeMillis();
    }

    @Override
    public boolean hasFailed() {
        return state == ERRORED;
    }

    public void reset() {
        state = IN_PROGRESS;
        errorMsg = null;
        startTime = System.currentTimeMillis();
        endTime = 0;
    }

    public void update(ShardOperation.State opState, String OpError) {
        state = opState;
        errorMsg = OpError;
        if (isDone()) {
            endTime = System.currentTimeMillis();
        }
    }

    @Override
    public String toString() {
        return String.format("{data=%s, state=%s, startTime=%d, endTime=%d}", opData, state, startTime, endTime);
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

        @Override
        public String toString() {
            return String.format(
                    "ParentOpId=%s Id='%s', subscriptionId='%s', shardId=%d", parentOpId, operationId, subscriptionId,
                    shardId
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
                    subscription.getConsumptionPolicy(), subscription.getRetryPolicy(), shard
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
                    subscription.getConsumptionPolicy(), subscription.getRetryPolicy(), shard
            );
        }

        @Override
        public String toString() {
            return String.format("Stop.OpData{%s}", super.toString());
        }
    }
}
