package com.flipkart.varadhi.verticles.controller;

import com.flipkart.varadhi.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.cluster.messages.ResponseMessage;
import com.flipkart.varadhi.controller.SuppressionManager;
import com.flipkart.varadhi.qos.entity.ClientLoadInfo;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

@Slf4j
public class TrafficDataHandler {
    private final SuppressionManager suppressionManager;

    public TrafficDataHandler(SuppressionManager suppressionManager) {
        this.suppressionManager = suppressionManager;
    }

    public CompletableFuture<ResponseMessage> handle(ClusterMessage message) {
        return suppressionManager.addTrafficDataAsync(message.getData(ClientLoadInfo.class))
                .thenApply(message::getResponseMessage);
    }

}
