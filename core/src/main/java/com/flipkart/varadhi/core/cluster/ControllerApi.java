package com.flipkart.varadhi.core.cluster;

import java.util.concurrent.CompletableFuture;


/**
 * Cluster Internal APIs.
 * Controller APIs for handling the events during entity lifecycle.
 * This will be generally invoked when a respective events is received from WebServer, Consumer(s) etc.
 */
public interface ControllerApi {
    String ROUTE_CONTROLLER = "controller";
    CompletableFuture<Void> startSubscription(SubscriptionOperation.StartData operation);
    CompletableFuture<Void> stopSubscription(SubscriptionOperation.StopData operation);
    CompletableFuture<Void> update(ShardOperation.OpData operation);
}
