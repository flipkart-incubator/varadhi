package com.flipkart.varadhi.verticles.controller;

import com.flipkart.varadhi.cluster.MessageExchange;
import com.flipkart.varadhi.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.core.cluster.ControllerApi;
import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.entities.cluster.SubscriptionOpRequest;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.entities.cluster.SubscriptionStatus;

import java.util.concurrent.CompletableFuture;

public class ControllerClient implements ControllerApi {
    private final MessageExchange exchange;

    public ControllerClient(MessageExchange exchange) {
        this.exchange = exchange;
    }

    @Override
    public CompletableFuture<SubscriptionStatus> getSubscriptionStatus(String subscriptionId, String requestedBy) {
        SubscriptionOpRequest opRequest = new SubscriptionOpRequest(subscriptionId, requestedBy);
        ClusterMessage message = ClusterMessage.of(opRequest);
        return exchange.request(ROUTE_CONTROLLER, "status", message)
                .thenApply(rm -> rm.getResponse(SubscriptionStatus.class));
    }

    @Override
    public CompletableFuture<SubscriptionOperation> startSubscription(String subscriptionId, String requestedBy) {
        SubscriptionOpRequest opRequest = new SubscriptionOpRequest(subscriptionId, requestedBy);
        ClusterMessage message = ClusterMessage.of(opRequest);
        return exchange.request(ROUTE_CONTROLLER, "start", message)
                .thenApply(rm -> rm.getResponse(SubscriptionOperation.class));
    }

    @Override
    public CompletableFuture<SubscriptionOperation> stopSubscription(String subscriptionId, String requestedBy) {
        SubscriptionOpRequest opRequest = new SubscriptionOpRequest(subscriptionId, requestedBy);
        ClusterMessage message = ClusterMessage.of(opRequest);
        return exchange.request(ROUTE_CONTROLLER, "stop", message)
                .thenApply(rm -> rm.getResponse(SubscriptionOperation.class));
    }

    @Override
    public CompletableFuture<Void> update(ShardOperation.OpData operation) {
        ClusterMessage msg = ClusterMessage.of(operation);
        return exchange.send(ROUTE_CONTROLLER, "update", msg);
    }

}
