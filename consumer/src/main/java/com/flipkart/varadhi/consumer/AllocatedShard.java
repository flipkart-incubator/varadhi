package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.entities.SubscriptionUnitShard;
import com.flipkart.varadhi.entities.cluster.ShardStatus;
import lombok.AllArgsConstructor;
import lombok.Data;


@Data
@AllArgsConstructor
class AllocatedShard {
    private final SubscriptionUnitShard shard;
    private ShardStatus status;
}
