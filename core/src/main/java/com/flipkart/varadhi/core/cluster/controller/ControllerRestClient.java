package com.flipkart.varadhi.core.cluster.controller;

import com.flipkart.varadhi.core.cluster.MessageExchange;
import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.core.subscription.allocation.ShardAssignments;
import com.flipkart.varadhi.core.subscription.SubscriptionOpRequest;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.core.subscription.UnsidelineOpRequest;
import com.flipkart.varadhi.entities.UnsidelineRequest;
import com.flipkart.varadhi.entities.cluster.*;
import com.flipkart.varadhi.entities.cluster.failover.TopicFailoverRequest;
import com.flipkart.varadhi.entities.cluster.failover.TopicFailoverTransition;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class ControllerRestClient implements ControllerApi {
    private final MessageExchange exchange;

    public ControllerRestClient(MessageExchange exchange) {
        this.exchange = exchange;
    }

    @Override
    public CompletableFuture<SubscriptionState> getSubscriptionState(String subscriptionId, String requestedBy) {
        SubscriptionOpRequest opRequest = new SubscriptionOpRequest(subscriptionId, requestedBy);
        ClusterMessage message = ClusterMessage.of(opRequest);
        return exchange.request(ROUTE_CONTROLLER, "state", message)
                       .thenApply(rm -> rm.getResponse(SubscriptionState.class));
    }

    @Override
    public CompletableFuture<SubscriptionOperation> startSubscription(String subscriptionId, String requestedBy) {
        SubscriptionOpRequest opRequest = new SubscriptionOpRequest(subscriptionId, requestedBy);
        ClusterMessage message = ClusterMessage.of(opRequest);
        return exchange.request(ROUTE_CONTROLLER, "start", message)
                       .thenApply(rm -> rm.getResponse(SubscriptionOperation.class));
    }

    @Override
    public CompletableFuture<SubscriptionOperation> stopSubscription(String subscriptionId, String requestedBy) {
        SubscriptionOpRequest opRequest = new SubscriptionOpRequest(subscriptionId, requestedBy);
        ClusterMessage message = ClusterMessage.of(opRequest);
        return exchange.request(ROUTE_CONTROLLER, "stop", message)
                       .thenApply(rm -> rm.getResponse(SubscriptionOperation.class));
    }

    @Override
    public CompletableFuture<SubscriptionOperation> unsideline(
        String subscriptionId,
        UnsidelineRequest request,
        String requestedBy
    ) {
        ClusterMessage message = ClusterMessage.of(new UnsidelineOpRequest(subscriptionId, requestedBy, request));
        return exchange.request(ROUTE_CONTROLLER, "unsideline", message)
                       .thenApply(rm -> rm.getResponse(SubscriptionOperation.class));
    }

    @Override
    public CompletableFuture<ShardAssignments> getShardAssignments(String subscriptionId) {
        ClusterMessage message = ClusterMessage.of(subscriptionId);
        return exchange.request(ROUTE_CONTROLLER, "getShards", message)
                       .thenApply(rm -> rm.getResponse(ShardAssignments.class));
    }

    /* ============== Topic Failover ============== */

    @Override
    public CompletableFuture<TopicFailoverTransition> createTopicFailover(
        String topicFqn,
        TopicFailoverRequest request,
        String requestedBy
    ) {
        ClusterMessage message = ClusterMessage.of(new TopicFailoverWireRequest(topicFqn, request, requestedBy));
        return exchange.request(ROUTE_CONTROLLER, "createFailover", message)
                       .thenApply(rm -> rm.getResponse(TopicFailoverTransition.class));
    }

    @Override
    @SuppressWarnings ("unchecked")
    public CompletableFuture<Optional<TopicFailoverTransition>> getTopicFailover(String topicFqn) {
        ClusterMessage message = ClusterMessage.of(topicFqn);
        return exchange.request(ROUTE_CONTROLLER, "getFailover", message)
                       .thenApply(rm -> Optional.ofNullable(rm.getResponse(TopicFailoverTransition.class)));
    }

    @Override
    public CompletableFuture<TopicFailoverTransition> abortTopicFailover(String topicFqn, String requestedBy) {
        ClusterMessage message = ClusterMessage.of(new TopicFailoverWireRequest(topicFqn, null, requestedBy));
        return exchange.request(ROUTE_CONTROLLER, "abortFailover", message)
                       .thenApply(rm -> rm.getResponse(TopicFailoverTransition.class));
    }

    @Override
    @SuppressWarnings ({"unchecked", "rawtypes"})
    public CompletableFuture<List<TopicFailoverTransition>> listActiveTopicFailovers() {
        ClusterMessage message = ClusterMessage.of();
        return exchange.request(ROUTE_CONTROLLER, "listActiveFailovers", message)
                       .thenApply(rm -> (List<TopicFailoverTransition>) rm.getResponse(List.class));
    }

    /** Wire envelope used by the three failover request endpoints. */
    public record TopicFailoverWireRequest(String topicFqn, TopicFailoverRequest request, String requestedBy) {}
}
