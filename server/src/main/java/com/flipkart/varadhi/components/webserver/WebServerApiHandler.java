package com.flipkart.varadhi.components.webserver;

import com.flipkart.varadhi.WebServerApiManager;
import com.flipkart.varadhi.cluster.messages.SubscriptionMessage;

import java.util.concurrent.CompletableFuture;

public class WebServerApiHandler {
    private final WebServerApiManager serverOpMgr;

    public WebServerApiHandler(WebServerApiManager serverOpMgr) {
        this.serverOpMgr = serverOpMgr;
    }

    public CompletableFuture<Void> update(SubscriptionMessage message) {
        return serverOpMgr.update(message.getOperation());
    }
}
