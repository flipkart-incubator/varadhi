package com.flipkart.varadhi.verticles.consumer;

import com.flipkart.varadhi.cluster.MessageExchange;
import com.flipkart.varadhi.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.entities.GetMessagesRequest;
import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.cluster.*;
import com.flipkart.varadhi.core.cluster.ConsumerApi;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;
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
    public CompletableFuture<Void> stop(ShardOperation.StopData operation) {
        ClusterMessage message = ClusterMessage.of(operation);
        log.debug("Sending message {}", message);
        return exchange.send(consumerId, "stop", message);
    }

    @Override
    public CompletableFuture<Void> unsideline(ShardOperation.UnsidelineData operation) {
        ClusterMessage message = ClusterMessage.of(operation);
        log.debug("Sending message {}", message);
        return exchange.send(consumerId, "unsideline", message);
    }

    @Override
    public CompletableFuture<Optional<ConsumerState>> getConsumerState(String subscriptionId, int shardId) {
        return exchange.request(
                        consumerId, "status", ClusterMessage.of(new ShardStatusRequest(subscriptionId, shardId)))
                .thenApply(rm -> Optional.ofNullable(rm.getResponse(ConsumerState.class)));
    }

    @Override
    public CompletableFuture<ConsumerInfo> getConsumerInfo() {
        ClusterMessage message = ClusterMessage.of();
        log.debug("Sending info request:{} {}", consumerId, message);
        return exchange.request(consumerId, "info", message)
                .thenApply(rm -> rm.getResponse(ConsumerInfo.class));
    }

    @Override
    public CompletableFuture<List<Message>> getMessages(GetMessagesRequest messagesRequest) {
        ClusterMessage message = ClusterMessage.of(messagesRequest);
        return exchange.request(consumerId, "getMessages", message)
                .thenApply(rm -> rm.getResponse(List.class));
    }
}
