package com.flipkart.varadhi.verticles.controller;

import com.flipkart.varadhi.cluster.MessageExchange;
import com.flipkart.varadhi.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.core.cluster.ControllerApi;
import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;

import java.util.concurrent.CompletableFuture;

public class ControllerApiProxy implements ControllerApi {
    private final MessageExchange exchange;

    public ControllerApiProxy(MessageExchange exchange) {
        this.exchange = exchange;
    }
    @Override
    public CompletableFuture<Void> startSubscription(SubscriptionOperation.StartData operation) {
        ClusterMessage message =  ClusterMessage.of(operation);
        return exchange.send(ROUTE_CONTROLLER, "start", message);
    }

    @Override
    public CompletableFuture<Void> stopSubscription(SubscriptionOperation.StopData operation) {
        ClusterMessage message =  ClusterMessage.of(operation);
        return exchange.send(ROUTE_CONTROLLER, "stop", message);
    }

    @Override
    public CompletableFuture<Void> update(ShardOperation.OpData operation) {
        ClusterMessage msg = ClusterMessage.of(operation);
        return exchange.send(ROUTE_CONTROLLER, "update", msg);
    }

}
