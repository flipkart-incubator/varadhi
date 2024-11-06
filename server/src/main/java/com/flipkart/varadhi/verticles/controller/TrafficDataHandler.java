package com.flipkart.varadhi.verticles.controller;

import com.flipkart.varadhi.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.cluster.messages.ResponseMessage;
import com.flipkart.varadhi.qos.DistributedRateLimiter;
import com.flipkart.varadhi.qos.entity.ClientLoadInfo;
import com.flipkart.varadhi.qos.entity.SuppressionData;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

@Slf4j
public class TrafficDataHandler {
    private final DistributedRateLimiter rateLimiter;

    public TrafficDataHandler(DistributedRateLimiter rateLimiter) {
        this.rateLimiter = rateLimiter;
    }

    public CompletableFuture<ResponseMessage> handle(ClusterMessage message) {
        SuppressionData suppressionData = rateLimiter.addTrafficData(message.getData(ClientLoadInfo.class));
        return CompletableFuture.completedFuture(suppressionData).thenApply(message::getResponseMessage);
    }

}
