package com.flipkart.varadhi.verticles.controller;

import com.flipkart.varadhi.cluster.MessageExchange;
import com.flipkart.varadhi.cluster.messages.SubscriptionMessage;
import com.flipkart.varadhi.core.cluster.ControllerApi;
import com.flipkart.varadhi.core.cluster.SubscriptionOperation;

import java.util.concurrent.CompletableFuture;

public class ControllerApiProxy implements ControllerApi {
    private final MessageExchange exchange;

    public ControllerApiProxy(MessageExchange exchange) {
        this.exchange = exchange;
    }
    @Override
    public CompletableFuture<Void> startSubscription(SubscriptionOperation.StartData operation) {
        SubscriptionMessage message = new SubscriptionMessage(operation);
        return exchange.send(ROUTE_CONTROLLER, "start", message);
    }

    @Override
    public CompletableFuture<Void> stopSubscription(SubscriptionOperation.StopData operation) {
        SubscriptionMessage message = new SubscriptionMessage(operation);
        return exchange.send(ROUTE_CONTROLLER, "stop", message);
    }

}
