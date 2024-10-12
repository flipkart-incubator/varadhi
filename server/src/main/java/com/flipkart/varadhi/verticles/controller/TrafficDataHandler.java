package com.flipkart.varadhi.verticles.controller;

import com.flipkart.varadhi.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.cluster.messages.ResponseMessage;
import com.flipkart.varadhi.qos.entity.ClientLoadInfo;
import com.flipkart.varadhi.qos.entity.SuppressionData;
import com.flipkart.varadhi.qos.entity.SuppressionFactor;
import com.flipkart.varadhi.qos.entity.TopicLoadInfo;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

@Slf4j
public class TrafficDataHandler {
    private final SuppressionManager suppressionManager;

    public TrafficDataHandler(SuppressionManager suppressionManager) {
        this.suppressionManager = suppressionManager;
    }

    public CompletableFuture<ResponseMessage> handle(ClusterMessage message) {
        ClientLoadInfo info = message.getData(ClientLoadInfo.class);
        SuppressionData suppressionData = new SuppressionData();
        long delta = System.currentTimeMillis() - info.getTo();
        log.info("Delta: {}ms", delta);

        info.getTopicUsageList().forEach((trafficData) -> {
            SuppressionFactor suppressionFactor = suppressionManager.addTrafficData(
                    info.getClientId(),
                    new TopicLoadInfo(info.getClientId(), info.getFrom(), info.getTo(), trafficData)
            );
            log.info("Topic: {}, SF thr-pt: {}", trafficData.getTopic(), suppressionFactor.getThroughputFactor());
            suppressionData.getSuppressionFactor().put(trafficData.getTopic(), suppressionFactor);
        });
        return CompletableFuture.completedFuture(message.getResponseMessage(suppressionData));
    }

}
