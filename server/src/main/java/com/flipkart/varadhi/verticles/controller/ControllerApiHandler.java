package com.flipkart.varadhi.verticles.controller;

import com.flipkart.varadhi.cluster.messages.ShardMessage;
import com.flipkart.varadhi.cluster.messages.SubscriptionMessage;
import com.flipkart.varadhi.core.cluster.ControllerApi;
import com.flipkart.varadhi.core.cluster.ShardOperation;
import com.flipkart.varadhi.core.cluster.SubscriptionOperation;
import com.flipkart.varadhi.core.cluster.WebServerApi;

import java.util.concurrent.CompletableFuture;

public class ControllerApiHandler {
    private final ControllerApi controllerMgr;
    private final WebServerApi webServerApiProxy;

    public ControllerApiHandler(ControllerApi controllerMgr, WebServerApi webServerApiProxy) {
        this.controllerMgr = controllerMgr;
        this.webServerApiProxy = webServerApiProxy;
    }

    public CompletableFuture<Void> start(SubscriptionMessage message) {
        SubscriptionOperation.StartData operation = (SubscriptionOperation.StartData) message.getOperation();
        return  controllerMgr.startSubscription(operation).exceptionally(throwable -> {
            //TODO:: is this exceptionally block correct, or should it be try/catch ?
            operation.markFail(throwable.getMessage());
            webServerApiProxy.update(operation);
            return null;
        });
    }

    public CompletableFuture<Void> stop(SubscriptionMessage message) {
        SubscriptionOperation.StopData operation = (SubscriptionOperation.StopData) message.getOperation();
        return  controllerMgr.stopSubscription(operation).exceptionally(throwable -> {
            operation.markFail(throwable.getMessage());
            webServerApiProxy.update(operation);
            return null;
        });
    }

    public CompletableFuture<Void> update(ShardMessage message) {
        ShardOperation.OpData operation = message.getOperation();
        return  controllerMgr.update(operation).exceptionally(throwable -> {
            //TODO::handle failure to update.
            return null;
        });
    }
}
