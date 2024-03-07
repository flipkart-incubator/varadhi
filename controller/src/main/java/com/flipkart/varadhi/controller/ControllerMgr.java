package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.core.cluster.messages.SubscriptionOperation;
import com.flipkart.varadhi.core.ophandlers.ControllerOpHandler;
import com.flipkart.varadhi.core.ophandlers.ServerOpHandler;
import com.flipkart.varadhi.core.proxies.ServerOpMgrProxy;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.spi.db.MetaStore;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.concurrent.CompletableFuture;

public class ControllerMgr implements ControllerOpHandler {
    private ServerOpHandler serverOpHandler;

    public ControllerMgr(ServerOpHandler serverOpHandler) {
        this.serverOpHandler = serverOpHandler;
    }
    @Override
    public CompletableFuture<Void> StartSubscription(SubscriptionOperation operation) {
        operation.markInProgress();
        serverOpHandler.update(operation);
        return CompletableFuture.completedFuture(null);
    }
}
