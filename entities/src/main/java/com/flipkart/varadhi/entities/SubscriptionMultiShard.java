package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import lombok.Getter;

import java.util.Map;

@Getter
public class SubscriptionMultiShard extends SubscriptionShards {
    private final Map<Integer, SubscriptionUnitShard> shards;

    @JsonCreator
    public SubscriptionMultiShard(Map<Integer, SubscriptionUnitShard> shards) {
        super(shards.size());
        this.shards = shards;
    }

    @Override
    public SubscriptionUnitShard getShard(int shardId) {
        if (shards.containsKey(shardId)) {
            return shards.get(shardId);
        }
        throw new IllegalArgumentException("Invalid shard Id, no shard found.");
    }
}
