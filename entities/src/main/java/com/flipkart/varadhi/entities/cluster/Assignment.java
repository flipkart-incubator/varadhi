package com.flipkart.varadhi.entities.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.flipkart.varadhi.entities.MetaStoreEntity;
import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode(callSuper = true)
public class Assignment extends MetaStoreEntity {
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
}
