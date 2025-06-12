package com.flipkart.varadhi.core.cluster.consumer;

import com.flipkart.varadhi.core.cluster.ConsumerInfo;
import com.flipkart.varadhi.core.subscription.ShardDlqMessageResponse;
import com.flipkart.varadhi.entities.cluster.ConsumerState;
import com.flipkart.varadhi.entities.cluster.ShardOperation;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface ConsumerApi {
    CompletableFuture<Void> start(ShardOperation.StartData operation);

    CompletableFuture<Void> stop(ShardOperation.StopData operation);

    CompletableFuture<Void> unsideline(ShardOperation.UnsidelineData operation);

    CompletableFuture<Optional<ConsumerState>> getConsumerState(String subscriptionId, int shardId);

    CompletableFuture<ConsumerInfo> getConsumerInfo();

    CompletableFuture<ShardDlqMessageResponse> getMessagesByTimestamp(long earliestFailedAt, int max_limit);

    CompletableFuture<ShardDlqMessageResponse> getMessagesByOffset(String pageMarkers, int max_limit);
}
