package com.flipkart.varadhi.controller;


import java.util.concurrent.CompletableFuture;

import com.flipkart.varadhi.core.cluster.MemberInfo;
import com.flipkart.varadhi.core.cluster.ControllerApi;
import com.flipkart.varadhi.core.cluster.SubscriptionOperation;
import com.flipkart.varadhi.core.cluster.WebServerApi;

public class ControllerApiMgr implements ControllerApi {
    private final WebServerApi webServerApiProxy;

    public ControllerApiMgr(WebServerApi webServerApiProxy) {
        this.webServerApiProxy = webServerApiProxy;
    }
    @Override
    public CompletableFuture<Void> startSubscription(SubscriptionOperation.StartData operation) {
        operation.markInProgress();
        return webServerApiProxy.update(operation);
    }

    @Override
    public CompletableFuture<Void> stopSubscription(SubscriptionOperation.StopData operation) {
        operation.markInProgress();
        return webServerApiProxy.update(operation);
    }

    public void memberLeft(String id) {
        //To be implemented later.
    }

    public void memberJoined(MemberInfo memberInfo) {
        //To be implemented later.
    }
}
