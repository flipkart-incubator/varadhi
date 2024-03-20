package com.flipkart.varadhi.core.ophandlers;

import com.flipkart.varadhi.core.cluster.messages.SubscriptionOperation;

import java.util.concurrent.CompletableFuture;

public interface WebServerApi {
    CompletableFuture<Void> update(SubscriptionOperation.OpData operation);
}
