package com.flipkart.varadhi;

import com.flipkart.varadhi.core.cluster.messages.SubscriptionOperation;
import com.flipkart.varadhi.core.ophandlers.ServerOpHandler;
import com.flipkart.varadhi.entities.VaradhiSubscription;

import java.util.concurrent.CompletableFuture;

public class ServerOpManager implements ServerOpHandler {
    @Override
    public CompletableFuture<Void> update(SubscriptionOperation operation) {
        // persist the operation change.
        return CompletableFuture.completedFuture(null);
    }
}
