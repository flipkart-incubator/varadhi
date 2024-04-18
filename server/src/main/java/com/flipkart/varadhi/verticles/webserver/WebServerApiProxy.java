package com.flipkart.varadhi.verticles.webserver;

import com.flipkart.varadhi.cluster.MessageExchange;
import com.flipkart.varadhi.cluster.messages.SubscriptionMessage;
import com.flipkart.varadhi.core.cluster.SubscriptionOperation;
import com.flipkart.varadhi.core.cluster.WebServerApi;

import java.util.concurrent.CompletableFuture;

public class WebServerApiProxy implements WebServerApi {
    private final MessageExchange exchange;

    public WebServerApiProxy(MessageExchange exchange) {
        this.exchange = exchange;
    }

    @Override
    public CompletableFuture<Void> update(SubscriptionOperation.OpData operation) {
        SubscriptionMessage message = new SubscriptionMessage(operation);
        return exchange.send(ROUTE_WEBSERVER, "update", message);
    }
}
