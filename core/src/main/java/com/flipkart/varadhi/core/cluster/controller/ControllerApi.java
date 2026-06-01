package com.flipkart.varadhi.core.cluster.controller;

import com.flipkart.varadhi.core.subscription.allocation.ShardAssignments;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.entities.UnsidelineRequest;
import com.flipkart.varadhi.entities.cluster.*;
import com.flipkart.varadhi.entities.cluster.failover.TopicFailoverRequest;
import com.flipkart.varadhi.entities.cluster.failover.TopicFailoverTransition;

import java.util.List;
import java.util.Optional;
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

    /* ============== Topic Failover ============== */

    /**
     * Start a topic failover. Atomically creates the FTO and the orchestration Op,
     * enqueues it, and returns the initial transition snapshot. The caller's
     * {@code 202 Accepted} response carries this snapshot.
     */
    CompletableFuture<TopicFailoverTransition> createTopicFailover(
        String topicFqn,
        TopicFailoverRequest request,
        String requestedBy
    );

    /** Fetch the current transition for {@code topicFqn}, if any. */
    CompletableFuture<Optional<TopicFailoverTransition>> getTopicFailover(String topicFqn);

    /**
     * Abort an in-progress failover. Honored only before SWITCH succeeds; after that
     * the call fails with {@code InvalidOperationForResourceException}.
     */
    CompletableFuture<TopicFailoverTransition> abortTopicFailover(String topicFqn, String requestedBy);

    /** Admin only: lists every non-terminal transition across the cluster. */
    CompletableFuture<List<TopicFailoverTransition>> listActiveTopicFailovers();
}
