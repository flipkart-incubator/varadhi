package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.core.cluster.messages.SubscriptionOperation;
import com.flipkart.varadhi.core.ophandlers.ControllerApi;
import com.flipkart.varadhi.core.ophandlers.WebServerApi;

import java.util.concurrent.CompletableFuture;

public class ControllerMgr implements ControllerApi {
    private final WebServerApi webServerApiProxy;

    public ControllerMgr(WebServerApi webServerApiProxy) {
        this.webServerApiProxy = webServerApiProxy;
    }
    @Override
    public CompletableFuture<Void> StartSubscription(SubscriptionOperation.StartData operation) {
        operation.markInProgress();
        return webServerApiProxy.update(operation);
    }
}
