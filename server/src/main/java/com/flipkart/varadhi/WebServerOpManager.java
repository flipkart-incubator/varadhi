package com.flipkart.varadhi;

import com.flipkart.varadhi.core.cluster.messages.SubscriptionOperation;
import com.flipkart.varadhi.core.ophandlers.WebServerApi;

import java.util.concurrent.CompletableFuture;

public class WebServerOpManager implements WebServerApi {
    @Override
    public CompletableFuture<Void> update(SubscriptionOperation.OpData operation) {
        // TODO:: persist the operation change.
        return CompletableFuture.completedFuture(null);
    }
}
