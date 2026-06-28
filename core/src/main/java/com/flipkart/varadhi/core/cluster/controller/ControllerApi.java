package com.flipkart.varadhi.core.cluster.controller;

import com.flipkart.varadhi.core.subscription.allocation.ShardAssignments;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.entities.cluster.TopicFailoverOperation;
import com.flipkart.varadhi.entities.cluster.failover.TopicFailoverRequest;
import com.flipkart.varadhi.entities.cluster.failover.TransitionObject;
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

    CompletableFuture<SubscriptionState> getSubscriptionState(String subscriptionId, String requestedBy);

    CompletableFuture<SubscriptionOperation> startSubscription(String subscriptionId, String requestedBy);

    CompletableFuture<SubscriptionOperation> stopSubscription(String subscriptionId, String requestedBy);

    CompletableFuture<SubscriptionOperation> unsideline(
        String subscriptionId,
        UnsidelineRequest request,
        String requestedBy
    );

    CompletableFuture<ShardAssignments> getShardAssignments(String subscriptionId);

    CompletableFuture<TopicFailoverOperation> createTopicFailover(String topicFqn, TopicFailoverRequest request);

    CompletableFuture<TransitionObject> getTopicFailover(String topicFqn);

    CompletableFuture<TransitionObject> abortTopicFailover(String topicFqn, String requestedBy);

    CompletableFuture<List<TransitionObject>> getActiveFailovers();
}
