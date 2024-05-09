package com.flipkart.varadhi.verticles.webserver;

import com.flipkart.varadhi.WebServerApiManager;
import com.flipkart.varadhi.cluster.messages.SubscriptionMessage;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

@Slf4j
public class WebServerApiHandler {
    private final WebServerApiManager serverOpMgr;

    public WebServerApiHandler(WebServerApiManager serverOpMgr) {
        this.serverOpMgr = serverOpMgr;
    }

    public CompletableFuture<Void> update(SubscriptionMessage message) {
        return serverOpMgr.update(message.getOperation());
    }
}
