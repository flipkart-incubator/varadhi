package com.flipkart.varadhi.core.ophandlers;

import com.flipkart.varadhi.core.cluster.messages.SubscriptionOperation;

import java.util.concurrent.CompletableFuture;

public interface ControllerApi {
    CompletableFuture<Void> StartSubscription(SubscriptionOperation.StartData operation);
}
