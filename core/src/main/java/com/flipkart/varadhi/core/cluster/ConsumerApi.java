package com.flipkart.varadhi.core.cluster;

import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.entities.cluster.ShardStatus;

import java.util.concurrent.CompletableFuture;

public interface ConsumerApi {
    CompletableFuture<Void> start(ShardOperation.StartData operation);

    CompletableFuture<ShardStatus> getStatus(String subscriptionId, int shardId);
}
