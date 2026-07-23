package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.controller.failover.TopicTransitionMetrics;
import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.core.cluster.messages.ResponseMessage;
import com.flipkart.varadhi.core.subscription.ShardOpResponse;
import com.flipkart.varadhi.core.subscription.SubscriptionOpRequest;
import com.flipkart.varadhi.core.subscription.UnsidelineOpRequest;
import com.flipkart.varadhi.entities.cluster.failover.TransitionAck;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

/**
 * Thin bus ingress for controller APIs. Delegates to {@link SubscriptionService} and
 * {@link TransitionService}.
 */
@Slf4j
public class ControllerHandler {

    private final SubscriptionService subscriptionService;
    private final TransitionService transitionService;
    private final TopicTransitionMetrics transitionMetrics;

    public ControllerHandler(
        SubscriptionService subscriptionService,
        TransitionService transitionService,
        TopicTransitionMetrics transitionMetrics
    ) {
        this.subscriptionService = subscriptionService;
        this.transitionService = transitionService;
        this.transitionMetrics = transitionMetrics;
    }

    public CompletableFuture<ResponseMessage> start(ClusterMessage message) {
        SubscriptionOpRequest request = message.getRequest(SubscriptionOpRequest.class);
        return subscriptionService.startSubscription(request.getSubscriptionId(), request.getRequestedBy())
                                  .thenApply(message::getResponseMessage);
    }

    public CompletableFuture<ResponseMessage> stop(ClusterMessage message) {
        SubscriptionOpRequest request = message.getRequest(SubscriptionOpRequest.class);
        return subscriptionService.stopSubscription(request.getSubscriptionId(), request.getRequestedBy())
                                  .thenApply(message::getResponseMessage);
    }

    public CompletableFuture<ResponseMessage> status(ClusterMessage message) {
        SubscriptionOpRequest request = message.getRequest(SubscriptionOpRequest.class);
        return subscriptionService.getSubscriptionState(request.getSubscriptionId(), request.getRequestedBy())
                                  .thenApply(message::getResponseMessage);
    }

    public CompletableFuture<ResponseMessage> unsideline(ClusterMessage message) {
        UnsidelineOpRequest request = message.getRequest(UnsidelineOpRequest.class);
        return subscriptionService.unsideline(
            request.getSubscriptionId(),
            request.getRequest(),
            request.getRequestedBy()
        ).thenApply(message::getResponseMessage);
    }

    public CompletableFuture<ResponseMessage> getShards(ClusterMessage message) {
        String subscriptionId = message.getRequest(String.class);
        return subscriptionService.getShardAssignments(subscriptionId).thenApply(message::getResponseMessage);
    }

    public void update(ClusterMessage message) {
        ShardOpResponse opResponse = message.getData(ShardOpResponse.class);
        subscriptionService.update(
            opResponse.getSubOpId(),
            opResponse.getShardOpId(),
            opResponse.getState(),
            opResponse.getErrorMsg()
        ).exceptionally(throwable -> {
            log.error("Shard update ({}) failed {}.", opResponse, throwable.getMessage());
            return null;
        });
    }

    public void ack(ClusterMessage message) {
        TransitionAck ack = message.getData(TransitionAck.class);
        transitionMetrics.ackReceived(ack.transitionType(), ack.stage());
        transitionService.ack(ack)
                         .thenRun(() -> transitionMetrics.ackProcessed(ack.transitionType(), ack.stage()))
                         .exceptionally(throwable -> {
                             transitionMetrics.ackDeliveryFailed(ack.transitionType(), ack.stage());
                             log.error("Topic-transition ack processing failed for ack={}", ack, throwable);
                             return null;
                         });
    }
}
