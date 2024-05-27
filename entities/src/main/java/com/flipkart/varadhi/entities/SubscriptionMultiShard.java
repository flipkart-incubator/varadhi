package com.flipkart.varadhi.entities;

import lombok.Getter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
public class SubscriptionMultiShard extends SubscriptionShards {
    private final Map<Integer, SubscriptionUnitShard> shards;

    public SubscriptionMultiShard(List<SubscriptionUnitShard> shards) {
        super(shards.size());
        this.shards = new HashMap<>();
        shards.forEach(shard -> this.shards.put(shard.getShardId(), shard));
    }

    @Override
    public SubscriptionUnitShard getShard(int shardId) {
        if (shards.containsKey(shardId)) {
            return shards.get(shardId);
        }
        throw new IllegalArgumentException("Invalid shard Id, no shard found.");
    }
}
