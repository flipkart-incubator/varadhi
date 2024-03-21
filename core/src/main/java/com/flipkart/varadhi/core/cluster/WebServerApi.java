package com.flipkart.varadhi.core.cluster;

import java.util.concurrent.CompletableFuture;

public interface WebServerApi {
    String ROUTE_WEBSERVER = "webserver";
    CompletableFuture<Void> update(SubscriptionOperation.OpData operation);
}
