package com.flipkart.varadhi.verticles.controller;

import com.flipkart.varadhi.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.cluster.messages.ResponseMessage;
import com.flipkart.varadhi.controller.ControllerApiMgr;
import com.flipkart.varadhi.core.cluster.entities.ShardOpResponse;
import com.flipkart.varadhi.core.cluster.entities.SubscriptionOpRequest;
import com.flipkart.varadhi.core.cluster.entities.UnsidelineOpRequest;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

@Slf4j
public class ControllerApiHandler {
    private final ControllerApiMgr controllerMgr;

    public ControllerApiHandler(ControllerApiMgr controllerMgr) {
        this.controllerMgr = controllerMgr;
    }

    public CompletableFuture<ResponseMessage> start(ClusterMessage message) {
        SubscriptionOpRequest request = message.getRequest(SubscriptionOpRequest.class);
        return controllerMgr.startSubscription(request.getSubscriptionId(), request.getRequestedBy())
                            .thenApply(message::getResponseMessage);
    }

    public CompletableFuture<ResponseMessage> stop(ClusterMessage message) {
        SubscriptionOpRequest request = message.getRequest(SubscriptionOpRequest.class);
        return controllerMgr.stopSubscription(request.getSubscriptionId(), request.getRequestedBy())
                            .thenApply(message::getResponseMessage);
    }

    public CompletableFuture<ResponseMessage> status(ClusterMessage message) {
        SubscriptionOpRequest request = message.getRequest(SubscriptionOpRequest.class);
        return controllerMgr.getSubscriptionState(request.getSubscriptionId(), request.getRequestedBy())
                            .thenApply(message::getResponseMessage);
    }

    public CompletableFuture<ResponseMessage> unsideline(ClusterMessage message) {
        UnsidelineOpRequest request = message.getRequest(UnsidelineOpRequest.class);
        return controllerMgr.unsideline(request.getSubscriptionId(), request.getRequest(), request.getRequestedBy())
                            .thenApply(message::getResponseMessage);
    }

    public CompletableFuture<ResponseMessage> getShards(ClusterMessage message) {
        String subscriptionId = message.getRequest(String.class);
        return controllerMgr.getShardAssignments(subscriptionId).thenApply(message::getResponseMessage);
    }

    public void update(ClusterMessage message) {
        ShardOpResponse opResponse = message.getData(ShardOpResponse.class);
        controllerMgr.update(
            opResponse.getSubOpId(),
            opResponse.getShardOpId(),
            opResponse.getState(),
            opResponse.getErrorMsg()
        ).exceptionally(throwable -> {
            log.error("Shard update ({}) failed {}.", opResponse, throwable.getMessage());
            return null;
        });
    }
}
