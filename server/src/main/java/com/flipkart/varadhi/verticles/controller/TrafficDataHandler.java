package com.flipkart.varadhi.verticles.controller;

import com.flipkart.varadhi.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.cluster.messages.ResponseMessage;
import com.flipkart.varadhi.qos.entity.LoadInfo;
import com.flipkart.varadhi.qos.entity.SuppressionData;

import com.flipkart.varadhi.qos.entity.SuppressionFactor;
import com.flipkart.varadhi.qos.server.SuppressionManager;

import java.util.concurrent.CompletableFuture;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TrafficDataHandler {
    private final SuppressionManager suppressionManager;

    public TrafficDataHandler(SuppressionManager suppressionManager) {
        this.suppressionManager = suppressionManager;
    }

    public CompletableFuture<ResponseMessage> handle(ClusterMessage message) {
        LoadInfo info = message.getData(LoadInfo.class);
        SuppressionData<SuppressionFactor> suppressionData = new SuppressionData<>();
        long delta = System.currentTimeMillis() - info.getTo();
        log.info("Delta: {}ms", delta);

        info.getTopicUsageMap().forEach((topic, trafficData) -> {
            Float throughputFactor = suppressionManager.addThroughput(info.getClientId(), topic, trafficData.getThroughputIn());
            Float qpsFactor = suppressionManager.addQPS(info.getClientId(), topic, trafficData.getRateIn());
            suppressionData.getSuppressionFactor().put(topic, new SuppressionFactor(throughputFactor, qpsFactor));
        });
        log.info("Suppression data: {}", suppressionData);
        CompletableFuture<ResponseMessage> responseMessageCompletableFuture =
                CompletableFuture.completedFuture(message.getResponseMessage(suppressionData));
        log.info("Response message: {}", responseMessageCompletableFuture);
        return responseMessageCompletableFuture;
    }

}
