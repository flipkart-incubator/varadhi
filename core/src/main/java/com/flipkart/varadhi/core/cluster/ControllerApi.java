package com.flipkart.varadhi.core.cluster;

import com.flipkart.varadhi.entities.SubscriptionShards;
import com.flipkart.varadhi.entities.UnsidelineRequest;
import com.flipkart.varadhi.entities.cluster.*;

import java.util.List;
import java.util.concurrent.CompletableFuture;


/**
 * Cluster Internal APIs.
 * Controller APIs for handling the events during entity lifecycle.
 * This will be generally invoked when a respective events is received from WebServer, Consumer(s) etc.
 */
public interface ControllerApi {
    String ROUTE_CONTROLLER = "controller";

    CompletableFuture<SubscriptionStatus> getSubscriptionStatus(String subscriptionId, String requestedBy);

    CompletableFuture<SubscriptionOperation> startSubscription(String subscriptionId, String requestedBy);

    CompletableFuture<SubscriptionOperation> stopSubscription(String subscriptionId, String requestedBy);

    CompletableFuture<Void> update(String subOpId, String shardOpId, ShardOperation.State state, String errorMsg);

    CompletableFuture<SubscriptionOperation> unsideline(String subscriptionId, UnsidelineRequest request, String requestedBy);

    CompletableFuture<ShardAssignments> getShardAssignments(String subscriptionId);
}
