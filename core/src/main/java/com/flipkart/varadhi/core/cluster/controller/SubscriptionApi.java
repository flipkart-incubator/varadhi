package com.flipkart.varadhi.core.cluster.controller;

import com.flipkart.varadhi.core.subscription.allocation.ShardAssignments;
import com.flipkart.varadhi.entities.UnsidelineRequest;
import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.entities.cluster.SubscriptionState;

import java.util.concurrent.CompletableFuture;

/**
 * Controller subscription lifecycle APIs (request/response over the controller route).
 */
public interface SubscriptionApi {

    CompletableFuture<SubscriptionState> getSubscriptionState(String subscriptionId, String requestedBy);

    CompletableFuture<SubscriptionOperation> startSubscription(String subscriptionId, String requestedBy);

    CompletableFuture<SubscriptionOperation> stopSubscription(String subscriptionId, String requestedBy);

    CompletableFuture<SubscriptionOperation> unsideline(
        String subscriptionId,
        UnsidelineRequest request,
        String requestedBy
    );

    CompletableFuture<ShardAssignments> getShardAssignments(String subscriptionId);

    CompletableFuture<Void> update(String subOpId, String shardOpId, ShardOperation.State state, String errorMsg);
}
