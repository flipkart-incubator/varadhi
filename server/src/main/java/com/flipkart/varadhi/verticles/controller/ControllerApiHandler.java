package com.flipkart.varadhi.verticles.controller;

import com.flipkart.varadhi.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.cluster.messages.ResponseMessage;
import com.flipkart.varadhi.core.cluster.ControllerApi;
import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.entities.cluster.SubscriptionOpRequest;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

@Slf4j
public class ControllerApiHandler {
    private final ControllerApi controllerMgr;

    public ControllerApiHandler(ControllerApi controllerMgr) {
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

    public void update(ClusterMessage message) {
        ShardOperation.OpData operation = message.getData(ShardOperation.OpData.class);
        controllerMgr.update(operation).exceptionally(throwable -> {
            log.error("Shard update ({}) failed.", operation);
            return null;
        });
    }
}
