package com.flipkart.varadhi.entities.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.flipkart.varadhi.entities.MetaStoreEntity;
import com.flipkart.varadhi.entities.SubscriptionUnitShard;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import lombok.*;

import java.util.UUID;

@Value
@EqualsAndHashCode(callSuper = true)
public class ShardOperation extends MetaStoreEntity {
    long startTime;
    long endTime;
    OpData opData;

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

    public static ShardOperation startOp(SubscriptionUnitShard shard, VaradhiSubscription subscription) {
        ShardOperation.OpData data = new ShardOperation.StartData(shard, subscription);
        return new ShardOperation(data);
    }

    public void update(ShardOperation.OpData updated) {
        //TODO::check & fix setters
        if (!opData.operationId.equals(updated.operationId)) {
            throw new IllegalArgumentException("Update failed. Operation Id mismatch.");
        }
        opData.errorMsg = updated.errorMsg;
        opData.state = updated.state;
    }

    @Override
    public String toString() {
        return String.format("ShardOperation{data=%s, startTime=%d, endTime=%d}", opData, startTime, endTime);
    }

    public enum State {
        SCHEDULED, ERRORED, COMPLETED, IN_PROGRESS
    }

    @Getter
    @Setter
    @AllArgsConstructor
    @NoArgsConstructor
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "@opDataType")
    @JsonSubTypes({@JsonSubTypes.Type(value = ShardOperation.StartData.class, name = "startShardData"),})
    public static class OpData {
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

        public void markInProgress() {
            state = ShardOperation.State.IN_PROGRESS;
        }

        @Override
        public String toString() {
            return String.format(
                    "OpData{Id='%s', shardId=%d, subscriptionId='%s', state=%s, errorMsg='%s'}",
                    operationId, shard.getShardId(), subscription.getName(),  state, errorMsg
            );
        }
    }


    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class StartData extends ShardOperation.OpData {
        StartData(SubscriptionUnitShard shard, VaradhiSubscription subscription) {
            super(UUID.randomUUID().toString(), shard.getShardId(), shard, subscription, State.SCHEDULED, null);
        }

        @Override
        public String toString() {
            return String.format("Start:%s", super.toString());
        }
    }
}
