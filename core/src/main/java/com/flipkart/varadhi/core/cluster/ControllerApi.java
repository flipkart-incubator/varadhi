package com.flipkart.varadhi.core.cluster;

import java.util.concurrent.CompletableFuture;

public interface ControllerApi {
    String ROUTE_CONTROLLER = "controller";
    CompletableFuture<Void> startSubscription(SubscriptionOperation.StartData operation);
    CompletableFuture<Void> stopSubscription(SubscriptionOperation.StopData operation);
}
