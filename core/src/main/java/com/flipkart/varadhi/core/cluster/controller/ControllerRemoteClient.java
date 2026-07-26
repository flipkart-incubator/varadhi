package com.flipkart.varadhi.core.cluster.controller;

import com.flipkart.varadhi.core.cluster.MessageExchange;
import com.flipkart.varadhi.core.cluster.failover.ActiveFailovers;
import com.flipkart.varadhi.core.cluster.failover.FailoverApiRequest;
import com.flipkart.varadhi.core.cluster.failover.TransitionBusAddress;
import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.core.subscription.ShardOpResponse;
import com.flipkart.varadhi.core.subscription.SubscriptionOpRequest;
import com.flipkart.varadhi.core.subscription.UnsidelineOpRequest;
import com.flipkart.varadhi.core.subscription.allocation.ShardAssignments;
import com.flipkart.varadhi.entities.UnsidelineRequest;
import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.entities.cluster.SubscriptionState;
import com.flipkart.varadhi.entities.cluster.TopicFailoverOperation;
import com.flipkart.varadhi.entities.cluster.failover.TopicFailoverRequest;
import com.flipkart.varadhi.entities.cluster.failover.TransitionAck;
import com.flipkart.varadhi.entities.cluster.failover.TransitionEvent;
import com.flipkart.varadhi.entities.cluster.failover.TransitionObject;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.flipkart.varadhi.core.cluster.controller.ControllerApi.ROUTE_CONTROLLER;

/**
 * Remote stub for {@link ControllerApi} + {@link ConsumerCallbackApi} over {@link MessageExchange}.
 *
 * <p>{@link #sendEvent} is unsupported — transition broadcasts originate on the controller and use
 * {@code TransitionService} in-process.
 */
public class ControllerRemoteClient implements ControllerApi, ConsumerCallbackApi {

    private final MessageExchange exchange;

    public ControllerRemoteClient(MessageExchange exchange) {
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

    @Override
    public CompletableFuture<TopicFailoverOperation> createTopicFailover(
        String topicFqn,
        TopicFailoverRequest request
    ) {
        FailoverApiRequest apiRequest = new FailoverApiRequest(
            topicFqn,
            request.sourceRegion(),
            request.targetRegion(),
            request.waitForReplicationLagToClear(),
            request.requestedBy()
        );
        ClusterMessage message = ClusterMessage.of(apiRequest);
        return exchange.request(ROUTE_CONTROLLER, TransitionBusAddress.CREATE_FAILOVER_API, message)
                       .thenApply(rm -> rm.getResponse(TopicFailoverOperation.class));
    }

    @Override
    public CompletableFuture<TransitionObject> getTopicFailover(String topicFqn) {
        ClusterMessage message = ClusterMessage.of(FailoverApiRequest.of(topicFqn));
        return exchange.request(ROUTE_CONTROLLER, TransitionBusAddress.GET_FAILOVER_API, message)
                       .thenApply(rm -> rm.getResponse(TransitionObject.class));
    }

    @Override
    public CompletableFuture<TransitionObject> abortTopicFailover(String topicFqn, String requestedBy) {
        ClusterMessage message = ClusterMessage.of(FailoverApiRequest.of(topicFqn, requestedBy));
        return exchange.request(ROUTE_CONTROLLER, TransitionBusAddress.ABORT_FAILOVER_API, message)
                       .thenApply(rm -> rm.getResponse(TransitionObject.class));
    }

    @Override
    public CompletableFuture<List<TransitionObject>> getActiveFailovers() {
        ClusterMessage message = ClusterMessage.of(FailoverApiRequest.of(null));
        return exchange.request(ROUTE_CONTROLLER, TransitionBusAddress.LIST_FAILOVERS_API, message)
                       .thenApply(rm -> rm.getResponse(ActiveFailovers.class).transitions());
    }

    @Override
    public CompletableFuture<Void> sendEvent(TransitionEvent event) {
        throw new UnsupportedOperationException("sendEvent is controller-local; call TransitionService in-process");
    }

    @Override
    public CompletableFuture<Void> ack(TransitionAck ack) {
        ClusterMessage message = ClusterMessage.of(ack);
        return exchange.send(ROUTE_CONTROLLER, TransitionBusAddress.STAGE_ACK_API, message);
    }

    @Override
    public CompletableFuture<Void> update(
        String subOpId,
        String shardOpId,
        ShardOperation.State state,
        String errorMsg
    ) {
        ClusterMessage msg = ClusterMessage.of(new ShardOpResponse(subOpId, shardOpId, state, errorMsg));
        return exchange.send(ROUTE_CONTROLLER, "update", msg);
    }
}
