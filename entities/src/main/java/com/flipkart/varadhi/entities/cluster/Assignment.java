package com.flipkart.varadhi.entities.cluster;

import com.flipkart.varadhi.entities.MetaStoreEntity;
import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode(callSuper = true)
public class Assignment extends MetaStoreEntity {
    private String subscriptionId;
    private int shardId;
    private String consumerId;

    public Assignment(String subscriptionId, int shardId, String consumerId) {
        super("", 0);
        this.subscriptionId = subscriptionId;
        this.shardId = shardId;
        this.consumerId = consumerId;
    }
}
