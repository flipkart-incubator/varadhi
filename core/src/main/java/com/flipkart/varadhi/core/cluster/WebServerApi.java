package com.flipkart.varadhi.core.cluster;

import java.util.concurrent.CompletableFuture;

/**
 * Cluster Internal APIs.
 * Web server APIs for handling the events during entity (Subscription as of now) lifecycle.
 * This will be generally invoked when a respective events from the controller is received.
 */
public interface WebServerApi {
    String ROUTE_WEBSERVER = "webserver";

    /**
     * Updates the status of an already scheduled operation and associated entity.
     */
    CompletableFuture<Void> update(SubscriptionOperation.OpData operation);
}
