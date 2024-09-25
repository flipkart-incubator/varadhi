package com.flipkart.varadhi.verticles.controller;

import com.flipkart.varadhi.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.cluster.messages.ResponseMessage;
import com.flipkart.varadhi.qos.entity.LoadInfo;
import com.flipkart.varadhi.qos.entity.SuppressionData;
import com.flipkart.varadhi.qos.entity.SuppressionFactor;
import com.flipkart.varadhi.qos.server.SuppressionManager;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

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
        double windowSizeInSeconds = (double) (info.getTo() - info.getFrom()) / 1000;

        info.getTopicUsageMap().forEach((topic, trafficData) -> {
            Double throughputFactor = suppressionManager.addThroughput(
                    info.getClientId(),
                    topic,
                    trafficData.getThroughputIn() / windowSizeInSeconds
            );
            Double qpsFactor = suppressionManager.addQPS(
                    info.getClientId(),
                    topic,
                    trafficData.getRateIn() / windowSizeInSeconds
            );
            log.info("Topic: {}, Throughput factor: {}, QPS factor: {}", topic, throughputFactor, qpsFactor);
            suppressionData.getSuppressionFactor().put(topic, new SuppressionFactor(throughputFactor, qpsFactor));
        });
        return CompletableFuture.completedFuture(message.getResponseMessage(suppressionData));
    }

}
