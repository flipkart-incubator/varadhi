package com.flipkart.varadhi.components.webserver;

import com.flipkart.varadhi.WebServerOpManager;
import com.flipkart.varadhi.core.cluster.messages.SubscriptionMessage;

import java.util.concurrent.CompletableFuture;

public class WebServerApiHandler {
    private final WebServerOpManager serverOpMgr;

    public WebServerApiHandler(WebServerOpManager serverOpMgr) {
        this.serverOpMgr = serverOpMgr;
    }

    public CompletableFuture<Void> update(SubscriptionMessage message) {
        return serverOpMgr.update(message.getOperation());
    }
}
