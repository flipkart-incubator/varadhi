package com.flipkart.varadhi.verticles.webserver;

import com.flipkart.varadhi.cluster.MessageExchange;
import com.flipkart.varadhi.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.core.cluster.WebServerApi;

import java.util.concurrent.CompletableFuture;

public class WebServerClient implements WebServerApi {
    private final MessageExchange exchange;

    public WebServerClient(MessageExchange exchange) {
        this.exchange = exchange;
    }

    @Override
    public CompletableFuture<Void> update(SubscriptionOperation.OpData operation) {
        ClusterMessage message = ClusterMessage.of(operation);
        return exchange.send(ROUTE_WEBSERVER, "update", message);
    }
}
