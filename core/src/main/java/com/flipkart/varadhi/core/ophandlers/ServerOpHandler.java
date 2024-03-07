package com.flipkart.varadhi.core.ophandlers;

import com.flipkart.varadhi.core.cluster.messages.SubscriptionOperation;
import com.flipkart.varadhi.entities.VaradhiSubscription;

import java.util.concurrent.CompletableFuture;

public interface ServerOpHandler {
    CompletableFuture<Void> update(SubscriptionOperation operation);
}
