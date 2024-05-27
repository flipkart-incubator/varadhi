package com.flipkart.varadhi.verticles.webserver;

import com.flipkart.varadhi.WebServerApiManager;
import com.flipkart.varadhi.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

@Slf4j
public class WebServerApiHandler {
    private final WebServerApiManager serverOpMgr;

    public WebServerApiHandler(WebServerApiManager serverOpMgr) {
        this.serverOpMgr = serverOpMgr;
    }

    public CompletableFuture<Void> update(ClusterMessage message) {
        SubscriptionOperation.OpData operation = message.getData(SubscriptionOperation.OpData.class);
        return serverOpMgr.update(operation);
    }
}
