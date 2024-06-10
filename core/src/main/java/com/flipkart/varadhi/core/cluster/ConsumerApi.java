package com.flipkart.varadhi.core.cluster;

import com.flipkart.varadhi.entities.cluster.ConsumerInfo;
import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.entities.cluster.ShardStatus;

import java.util.concurrent.CompletableFuture;

public interface ConsumerApi {
    CompletableFuture<Void> start(ShardOperation.StartData operation);

    CompletableFuture<Void> stop(ShardOperation.StopData operation);

    CompletableFuture<ShardStatus> getShardStatus(String subscriptionId, int shardId);

    CompletableFuture<ConsumerInfo> getConsumerInfo();
}
