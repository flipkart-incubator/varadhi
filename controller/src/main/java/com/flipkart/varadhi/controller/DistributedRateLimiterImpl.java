package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.core.capacity.TopicCapacityService;
import com.flipkart.varadhi.qos.DistributedRateLimiter;
import com.flipkart.varadhi.qos.entity.ClientHistory;
import com.flipkart.varadhi.qos.entity.ClientLoadInfo;
import com.flipkart.varadhi.qos.entity.SuppressionData;
import com.flipkart.varadhi.qos.entity.SuppressionFactor;
import com.flipkart.varadhi.qos.entity.TopicLoadInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.mutable.MutableDouble;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class DistributedRateLimiterImpl implements DistributedRateLimiter {
    private Map<String, ClientHistory> topicTrafficDataMap; // topic to client load info
    private final int windowSlots;
    private final int singleSlotMillis;
    private final TopicCapacityService topicCapacityService;
    private final Clock clock;

    public DistributedRateLimiterImpl(int windowSlots, int singleSlotMillis, TopicCapacityService topicCapacityService, Clock clock) {
        this.topicTrafficDataMap = new ConcurrentHashMap<>();
        this.windowSlots = windowSlots;
        this.singleSlotMillis = singleSlotMillis;
        this.topicCapacityService = topicCapacityService;
        this.clock = clock;
    }

    // Adds throughput for current client and returns the updated suppression factor for the topic
    // TODO(rl): add NFR tests
    private SuppressionFactor addTrafficData(String clientId, TopicLoadInfo topicLoadInfo) {
        // check if clientId is already present in the list
        String topic = topicLoadInfo.topicLoad().topic();
        MutableDouble actualThroughout = new MutableDouble(0.0);
        topicTrafficDataMap.compute(topic, (k, v) -> {
            if (v == null) {
                v = new ClientHistory(windowSlots, singleSlotMillis, clock);
            }
            v.add(clientId, topicLoadInfo);
            actualThroughout.setValue(getThroughput(v));
            return v;
        });
        int throughputBPS = topicCapacityService.getThroughputLimit(topic);
        return new SuppressionFactor(calculateSuppressionFactor(throughputBPS, actualThroughout.getValue()));
    }

    // TODO(rl): remove client from here

    private Double calculateSuppressionFactor(double limit, double actual) {
        return Math.max(0, 1.0 - (limit / actual));
    }

    private Double getThroughput(ClientHistory clientsHistory) {
        double totalThroughput = 0.0;
        List<TopicLoadInfo> records = clientsHistory.predictLoad();
        for(TopicLoadInfo record: records){
            double windowSizeInSeconds = (double) (record.to() - record.from()) / 1000;
            totalThroughput += record.topicLoad().bytesIn() / windowSizeInSeconds;
        }
        return totalThroughput;
    }

    @Override
    public SuppressionData addTrafficData(ClientLoadInfo info) {
        SuppressionData suppressionData = new SuppressionData();
        info.getTopicUsageList().forEach((trafficData) -> {
            SuppressionFactor suppressionFactor = addTrafficData(
                    info.getClientId(),
                    new TopicLoadInfo(info.getClientId(), info.getFrom(), info.getTo(), trafficData)
            );
            log.debug("Topic: {}, SF thr-pt: {}", trafficData.topic(), suppressionFactor.getThroughputFactor());
            suppressionData.getSuppressionFactor().put(trafficData.topic(), suppressionFactor);
        });
        return suppressionData;
    }
}
