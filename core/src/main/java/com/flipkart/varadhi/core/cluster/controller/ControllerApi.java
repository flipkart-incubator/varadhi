package com.flipkart.varadhi.core.cluster.controller;

import com.flipkart.varadhi.entities.cluster.TopicFailoverOperation;
import com.flipkart.varadhi.entities.cluster.failover.TopicFailoverRequest;
import com.flipkart.varadhi.entities.cluster.failover.TransitionMaster;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Cluster-facing controller API: subscription lifecycle + topic-transition coordination.
 * Consumer shard callbacks live on {@link ConsumerCallbackApi}.
 */
public interface ControllerApi extends SubscriptionApi, TransitionApi {
    String ROUTE_CONTROLLER = "controller";

    CompletableFuture<TopicFailoverOperation> createTopicFailover(
        String topicFqn,
        TopicFailoverRequest request,
        String requestedBy
    );

    /** In-flight failover op (includes {@code stageHistory}); 404 if no active master. */
    CompletableFuture<TopicFailoverOperation> getTopicFailover(String topicFqn);

    CompletableFuture<TopicFailoverOperation> abortTopicFailover(String topicFqn, String requestedBy);

    CompletableFuture<List<TransitionMaster>> getActiveFailovers();
}
