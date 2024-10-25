package com.flipkart.varadhi.verticles.controller;

import com.flipkart.varadhi.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.cluster.messages.ResponseMessage;
import com.flipkart.varadhi.controller.SuppressionManager;
import com.flipkart.varadhi.qos.entity.ClientLoadInfo;
import com.flipkart.varadhi.qos.entity.SuppressionData;
import com.flipkart.varadhi.qos.entity.SuppressionFactor;
import com.flipkart.varadhi.qos.entity.TopicLoadInfo;
import com.flipkart.varadhi.utils.FutureUtil;

import java.util.ArrayList;
import java.util.List;

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

        List<CompletableFuture<SuppressionFactor>> suppressionFactorFuture = new ArrayList<>();

        info.getTopicUsageList().forEach((trafficData) -> {
            suppressionFactorFuture.add(suppressionManager.addTrafficData(
                    info.getClientId(),
                    new TopicLoadInfo(info.getClientId(), info.getFrom(), info.getTo(), trafficData)
            ).whenComplete((suppressionFactor, throwable) -> {
                if (throwable != null) {
                    log.error("Error while calculating suppression factor", throwable);
                    return;
                }
                log.info("Topic: {}, SF thr-pt: {}", trafficData.getTopic(), suppressionFactor.getThroughputFactor());
                suppressionData.getSuppressionFactor().put(trafficData.getTopic(), suppressionFactor);
            }));
        });

        return FutureUtil.waitForAll(suppressionFactorFuture).thenApply(__ -> message.getResponseMessage(suppressionData));
    }

}
