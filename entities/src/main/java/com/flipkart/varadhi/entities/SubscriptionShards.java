package com.flipkart.varadhi.entities;


import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
@JsonTypeInfo (use = JsonTypeInfo.Id.NAME, property = "@shardsStrategy")
@JsonSubTypes ({@JsonSubTypes.Type (value = SubscriptionUnitShard.class, name = "unit"), @JsonSubTypes.Type (
    value = SubscriptionMultiShard.class, name = "multi")})
public abstract class SubscriptionShards {
    private final int shardCount;

    public abstract SubscriptionUnitShard getShard(int shardId);
}
