package com.flipkart.varadhi.core.cluster.controller;

import com.flipkart.varadhi.entities.cluster.TopicFailoverOperation;
import com.flipkart.varadhi.entities.cluster.failover.TopicFailoverRequest;
import com.flipkart.varadhi.entities.cluster.failover.TransitionObject;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Cluster-facing controller API: subscription lifecycle + topic-transition coordination.
 * Consumer shard callbacks live on {@link ConsumerCallbackApi}.
 */
public interface ControllerApi extends SubscriptionApi, TransitionApi {
    String ROUTE_CONTROLLER = "controller";

    CompletableFuture<TopicFailoverOperation> createTopicFailover(String topicFqn, TopicFailoverRequest request);

    CompletableFuture<TransitionObject> getTopicFailover(String topicFqn);

    CompletableFuture<TransitionObject> abortTopicFailover(String topicFqn, String requestedBy);

    CompletableFuture<List<TransitionObject>> getActiveFailovers();
}
