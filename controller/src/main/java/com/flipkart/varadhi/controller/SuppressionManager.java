package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.qos.entity.ClientHistory;
import com.flipkart.varadhi.qos.entity.SuppressionFactor;
import com.flipkart.varadhi.qos.entity.TopicLoadInfo;
import com.google.common.base.Ticker;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.mutable.MutableDouble;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class SuppressionManager {
    private Map<String, ClientHistory<TopicLoadInfo>> topicTrafficDataMap; // topic to client load info
    private int windowSize;
    private TopicLimitService topicLimitService;
    private Ticker ticker;

    public SuppressionManager(int windowSize, TopicLimitService topicLimitService, Ticker ticker) {
        this.topicTrafficDataMap = new ConcurrentHashMap<>();
        this.windowSize = windowSize;
        this.topicLimitService = topicLimitService;
        this.ticker = ticker;
    }

    // Adds throughput for current client and returns the updated suppression factor for the topic
    // TODO(rl): add NFR tests
    // TODO(rl): cache for specific clientId and maintain a running agg.
    public CompletableFuture<SuppressionFactor> addTrafficData(String clientId, TopicLoadInfo topicLoadInfo) {
        return CompletableFuture.supplyAsync(() -> {
            // check if clientId is already present in the list
            String topic = topicLoadInfo.getTopicLoad().getTopic();
            MutableDouble actualThroughout = new MutableDouble(0.0);
            topicTrafficDataMap.compute(topic, (k, v) -> {
                if (v == null) {
                    v = new ClientHistory<>(windowSize);
                }
                v.add(clientId, topicLoadInfo);
                actualThroughout.setValue(getThroughput(v));
                return v;
            });
            int throughputBPS = topicLimitService.getThroughput(topic);
            log.info("Actual throughput: {}, Limit: {}", actualThroughout, throughputBPS);
            return new SuppressionFactor(calculateSuppressionFactor(throughputBPS, actualThroughout.getValue()));
        });
    }

    // TODO(rl): remove client from here

    private Double calculateSuppressionFactor(double limit, double actual) {
        return Math.max(0, 1.0 - (limit / actual));
    }

    private Double getThroughput(ClientHistory<TopicLoadInfo> clientsHistory) {
        double totalThroughput = 0.0;
        List<TopicLoadInfo> records = clientsHistory.getRecentRecordForAll();
        for(TopicLoadInfo record: records){
            // check if record is not too old
            if(isExpired(record.getTo())) {
                continue;
            }
            double windowSizeInSeconds = (double) (record.getTo() - record.getFrom()) / 1000;
            totalThroughput += record.getTopicLoad().getBytesIn() / windowSizeInSeconds;
        }
        return totalThroughput;
    }

    /**
     * check if record is older than windowSize
     * @param time record time
     * @return true if record is older than windowSize
     */
    private boolean isExpired(long time) {
        return (ticker.read() - windowSize * 1000L) > time;
    }

}
