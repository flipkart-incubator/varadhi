package com.flipkart.varadhi.entities;

import lombok.Getter;

@Getter
public class Subscription extends MetaStoreEntity {

    private String project;
    private String topic;
    private String description;
    private boolean grouped;
    private Endpoint endpoint;
    private RetryPolicy retryPolicy;
    private ConsumptionPolicy consumptionPolicy;
    private SubscriptionShard[] shards;

    public Subscription(
            String name,
            int version,
            String project,
            String topic,
            String description,
            boolean grouped,
            Endpoint endpoint,
            RetryPolicy retryPolicy,
            ConsumptionPolicy consumptionPolicy,
            SubscriptionShard[] shards
    ) {
        super(name, version);
        this.project = project;
        this.topic = topic;
        this.description = description;
        this.grouped = grouped;
        this.endpoint = endpoint;
        this.retryPolicy = retryPolicy;
        this.consumptionPolicy = consumptionPolicy;
        if (shards == null || shards.length == 0) {
            throw new IllegalArgumentException("shards cannot be null or empty");
        }
        this.shards = shards;
    }
}
