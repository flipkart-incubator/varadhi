package com.flipkart.varadhi.verticles.controller;

import com.flipkart.varadhi.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.core.cluster.ControllerApi;
import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.core.cluster.WebServerApi;

import java.util.concurrent.CompletableFuture;

public class ControllerApiHandler {
    private final ControllerApi controllerMgr;
    private final WebServerApi webServerApiProxy;

    public ControllerApiHandler(ControllerApi controllerMgr, WebServerApi webServerApiProxy) {
        this.controllerMgr = controllerMgr;
        this.webServerApiProxy = webServerApiProxy;
    }

    public CompletableFuture<Void> start(ClusterMessage message) {
        SubscriptionOperation.StartData operation = message.getData(SubscriptionOperation.StartData.class);
        return controllerMgr.startSubscription(operation).exceptionally(throwable -> {
            //TODO:: is this exceptionally block correct, or should it be try/catch ?
            operation.markFail(throwable.getMessage());
            webServerApiProxy.update(operation);
            return null;
        });
    }

    public CompletableFuture<Void> stop(ClusterMessage message) {
        SubscriptionOperation.StopData operation = message.getData(SubscriptionOperation.StopData.class);
        return controllerMgr.stopSubscription(operation).exceptionally(throwable -> {
            operation.markFail(throwable.getMessage());
            webServerApiProxy.update(operation);
            return null;
        });
    }

    public CompletableFuture<Void> update(ClusterMessage message) {
        ShardOperation.OpData operation = message.getData(ShardOperation.OpData.class);
        return controllerMgr.update(operation).exceptionally(throwable -> {
            //TODO::handle failure to update.
            return null;
        });
    }
}
