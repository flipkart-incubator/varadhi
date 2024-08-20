package com.flipkart.varadhi.verticles.controller;

import com.flipkart.varadhi.cluster.MessageExchange;
import com.flipkart.varadhi.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.core.cluster.ControllerApi;
import com.flipkart.varadhi.entities.cluster.*;

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
    public CompletableFuture<Void> update(
            String subOpId, String shardOpId, ShardOperation.State state, String errorMsg
    ) {
        ClusterMessage msg = ClusterMessage.of(new ShardOpResponse(subOpId, shardOpId, state, errorMsg));
        return exchange.send(ROUTE_CONTROLLER, "update", msg);
    }

}
