package com.flipkart.varadhi.entities.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.flipkart.varadhi.entities.Versioned;
import lombok.Value;

import java.util.Objects;

@Value
public class Assignment extends Versioned {
    private static final String NAME_SEPARATOR = ":";
    String subscriptionId;
    int shardId;
    String consumerId;

    @JsonCreator
    Assignment(String name, String subscriptionId, int shardId, String consumerId, int version) {
        super(name, version);
        this.subscriptionId = subscriptionId;
        this.shardId = shardId;
        this.consumerId = consumerId;
    }

    public Assignment(String subscriptionId, int shardId, String consumerId) {
        super(getShardName(subscriptionId, shardId), 0);
        this.subscriptionId = subscriptionId;
        this.shardId = shardId;
        this.consumerId = consumerId;
    }

    private static String getShardName(String subscriptionId, int shardId) {
        return String.format("%s%s%d", subscriptionId, NAME_SEPARATOR, shardId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;
        Assignment that = (Assignment)o;
        return shardId == that.shardId && Objects.equals(subscriptionId, that.subscriptionId) && Objects.equals(
            consumerId,
            that.consumerId
        );
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((consumerId == null) ? 0 : consumerId.hashCode());
        result = prime * result + shardId;
        result = prime * result + ((subscriptionId == null) ? 0 : subscriptionId.hashCode());
        return result;
    }

    @Override
    public String toString() {
        return String.format("{ConsumerId:%s Subscription:%s Shard:%d}", consumerId, subscriptionId, shardId);
    }
}
