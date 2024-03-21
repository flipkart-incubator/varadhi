package com.flipkart.varadhi.components.controller;

import com.flipkart.varadhi.cluster.messages.SubscriptionMessage;
import com.flipkart.varadhi.core.cluster.ControllerApi;
import com.flipkart.varadhi.core.cluster.SubscriptionOperation;
import com.flipkart.varadhi.core.cluster.WebServerApi;

import java.util.concurrent.CompletableFuture;

public class ControllerApiHandler {
    private final ControllerApi opHandler;
    private final WebServerApi webServerApiProxy;

    public ControllerApiHandler(ControllerApi opHandler, WebServerApi webServerApiProxy) {
        this.opHandler = opHandler;
        this.webServerApiProxy = webServerApiProxy;
    }

    public CompletableFuture<Void> start(SubscriptionMessage message) {
        SubscriptionOperation.StartData operation = (SubscriptionOperation.StartData) message.getOperation();
        return  opHandler.startSubscription(operation).exceptionally(throwable -> {
            operation.markFail(throwable.getMessage());
            webServerApiProxy.update(operation);
            return null;
        });
    }

    public CompletableFuture<Void> stop(SubscriptionMessage message) {
        SubscriptionOperation.StopData operation = (SubscriptionOperation.StopData) message.getOperation();
        return  opHandler.stopSubscription(operation).exceptionally(throwable -> {
            operation.markFail(throwable.getMessage());
            webServerApiProxy.update(operation);
            return null;
        });
    }
}
