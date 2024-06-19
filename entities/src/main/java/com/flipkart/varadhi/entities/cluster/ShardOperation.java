package com.flipkart.varadhi.entities.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.flipkart.varadhi.entities.MetaStoreEntity;
import com.flipkart.varadhi.entities.SubscriptionUnitShard;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import lombok.*;

import java.util.UUID;

@Getter
@EqualsAndHashCode(callSuper = true)
public class ShardOperation extends MetaStoreEntity implements  GroupOperation {
    private final long startTime;
    private long endTime;
    private final OpData opData;

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

    public static ShardOperation startOp(String subOpId, SubscriptionUnitShard shard, VaradhiSubscription subscription) {
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
    public String getGroupId() {
        return "Shard_" +  opData.getSubscription().getName() + "_" + String.valueOf(opData.shardId);
    }

    @JsonIgnore
    @Override
    public boolean isDone() {
        return opData.state == State.ERRORED || opData.state == State.COMPLETED;
    }

    public void update(ShardOperation.OpData updated) {
        if (!opData.operationId.equals(updated.operationId)) {
            throw new IllegalArgumentException("Update failed. Operation Id mismatch.");
        }
        opData.errorMsg = updated.errorMsg;
        opData.state = updated.state;
    }

    public void markFail(String reason) {
        opData.markFail(reason);
        endTime = System.currentTimeMillis();
    }

    public boolean hasFailed() {
        return opData.state == State.ERRORED;
    }

    @Override
    public String toString() {
        return String.format("{data=%s, startTime=%d, endTime=%d}", opData, startTime, endTime);
    }

    public enum State {
        SCHEDULED, ERRORED, COMPLETED, IN_PROGRESS
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
        private String parentOpId;
        private String operationId;
        private int shardId;
        private SubscriptionUnitShard shard;
        private VaradhiSubscription subscription;
        private ShardOperation.State state;
        private String errorMsg;

        public void markFail(String reason) {
            state = ShardOperation.State.ERRORED;
            errorMsg = reason;
        }

        @Override
        public String toString() {
            return String.format(
                    "OpData{ParentOpId=%s Id='%s', subscriptionId='%s', shardId=%d, state=%s, errorMsg='%s'}",
                    parentOpId, operationId, subscription.getName(), shard.getShardId(), state, errorMsg
            );
        }
    }


    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class StartData extends ShardOperation.OpData {
        StartData(String subOpId, SubscriptionUnitShard shard, VaradhiSubscription subscription) {
            super(subOpId, UUID.randomUUID().toString(), shard.getShardId(), shard, subscription, State.SCHEDULED, null);
        }

        @Override
        public String toString() {
            return String.format("Start.%s", super.toString());
        }
    }

    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class StopData extends ShardOperation.OpData {
        StopData(String subOpId, SubscriptionUnitShard shard, VaradhiSubscription subscription) {
            super(subOpId, UUID.randomUUID().toString(), shard.getShardId(), shard, subscription, State.SCHEDULED, null);
        }

        @Override
        public String toString() {
            return String.format("Stop.%s", super.toString());
        }
    }
}
