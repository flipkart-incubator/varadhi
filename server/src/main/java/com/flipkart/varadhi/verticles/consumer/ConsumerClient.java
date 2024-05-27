package com.flipkart.varadhi.verticles.consumer;

import com.flipkart.varadhi.cluster.MessageExchange;
import com.flipkart.varadhi.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.entities.cluster.ShardRequest;
import com.flipkart.varadhi.core.cluster.ConsumerApi;
import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.entities.cluster.ShardStatus;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

@Slf4j
public class ConsumerClient implements ConsumerApi {
    private final MessageExchange exchange;
    private final String consumerId;

    public ConsumerClient(String consumerId, MessageExchange exchange) {
        this.consumerId = consumerId;
        this.exchange = exchange;
    }

    @Override
    public CompletableFuture<Void> start(ShardOperation.StartData operation) {
        ClusterMessage message = ClusterMessage.of(operation);
        log.debug("Sending message {}", message);
        return exchange.send(consumerId, "start", message);
    }

    @Override
    public CompletableFuture<ShardStatus> getStatus(String subscriptionId, int shardId) {
        return exchange.request(consumerId, "status", ClusterMessage.of(new ShardRequest(subscriptionId, shardId)))
                .thenApply(responseMessage -> responseMessage.getResponse(ShardStatus.class));
    }
}
