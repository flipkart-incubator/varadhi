package com.flipkart.varadhi.core.cluster.controller;

import com.flipkart.varadhi.core.cluster.MessageExchange;
import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.core.subscription.ShardOpResponse;
import com.flipkart.varadhi.entities.cluster.ShardOperation;

import java.util.concurrent.CompletableFuture;

public class ControllerConsumerClient implements ControllerConsumerApi {
    private final MessageExchange exchange;

    public ControllerConsumerClient(MessageExchange exchange) {
        this.exchange = exchange;
    }

    @Override
    public CompletableFuture<Void> update(
        String subOpId,
        String shardOpId,
        ShardOperation.State state,
        String errorMsg
    ) {
        ClusterMessage msg = ClusterMessage.of(new ShardOpResponse(subOpId, shardOpId, state, errorMsg));
        return exchange.send(ROUTE_CONTROLLER, "update", msg);
    }
}
