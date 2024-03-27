package com.flipkart.varadhi.components.controller;

import com.flipkart.varadhi.core.cluster.messages.SubscriptionMessage;
import com.flipkart.varadhi.core.cluster.messages.SubscriptionOperation;
import com.flipkart.varadhi.core.ophandlers.ControllerApi;
import com.flipkart.varadhi.core.ophandlers.WebServerApi;

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
        return  opHandler.StartSubscription(operation).exceptionally(throwable -> {
            operation.markFail(throwable.getMessage());
            webServerApiProxy.update(operation);
            return null;
        });
    }
}
