package com.flipkart.varadhi.core.cluster.controller;

import com.flipkart.varadhi.entities.cluster.TopicFailoverOperation;
import com.flipkart.varadhi.entities.cluster.failover.TopicFailoverRequest;
import com.flipkart.varadhi.entities.cluster.failover.TransitionMaster;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Cluster-facing controller API: subscription lifecycle + topic-transition acks + failover RPCs.
 * Consumer shard callbacks live on {@link SubscriptionApi#update}.
 *
 * <p>Broadcast of transition stage events is controller-local ({@link TransitionPublisher}),
 * not part of this remote-callable surface.
 */
public interface ControllerApi extends SubscriptionApi, TransitionAckApi {
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
