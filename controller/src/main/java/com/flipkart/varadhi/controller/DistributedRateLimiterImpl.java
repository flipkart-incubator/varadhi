package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.core.capacity.TopicCapacityService;
import com.flipkart.varadhi.qos.DistributedRateLimiter;
import com.flipkart.varadhi.qos.entity.*;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Deque;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.mutable.MutableDouble;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class DistributedRateLimiterImpl implements DistributedRateLimiter {
    private Map<String, ClientHistory> topicTrafficDataMap; // topic to client load info
    private int windowSize;
    private TopicCapacityService topicCapacityService;
    private Clock clock;
    private final LoadPrediction loadPredictor;

    public DistributedRateLimiterImpl(int windowSize, TopicCapacityService topicCapacityService, Clock clock) {
        this.topicTrafficDataMap = new ConcurrentHashMap<>();
        this.windowSize = windowSize;
        this.topicCapacityService = topicCapacityService;
        this.clock = clock;
        this.loadPredictor = new LoadPrediction() {
            @Override
            public List<TopicLoadInfo> predictLoad(Map<String, Deque<TopicLoadInfo>> records) {
                List<TopicLoadInfo> recentRecords = new ArrayList<>();
                records.forEach((clientId, history) -> {
                    if(!history.isEmpty()) {
                        TopicLoadInfo topicLoadInfo = history.peekLast();
                        if(topicLoadInfo != null) {
                            if(!isExpired(topicLoadInfo.getTo())) {
                                recentRecords.add(topicLoadInfo);
                            }
                        }
                    }
                });
                return recentRecords;
            }
        };
    }

    // Adds throughput for current client and returns the updated suppression factor for the topic
    // TODO(rl): add NFR tests
    // TODO(rl): cache for specific clientId and maintain a running agg.
    public SuppressionFactor addTrafficData(String clientId, TopicLoadInfo topicLoadInfo) {
        // check if clientId is already present in the list
        String topic = topicLoadInfo.getTopicLoad().getTopic();
        MutableDouble actualThroughout = new MutableDouble(0.0);
        topicTrafficDataMap.compute(topic, (k, v) -> {
            if (v == null) {
                v = new ClientHistory(windowSize, loadPredictor);
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
        List<TopicLoadInfo> records = clientsHistory.getTotalLoad();
        for(TopicLoadInfo record: records){
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
        return (clock.millis() - windowSize * 1000L) > time;
    }

    @Override
    public SuppressionData addTrafficData(ClientLoadInfo info) {
        SuppressionData suppressionData = new SuppressionData();
        info.getTopicUsageList().forEach((trafficData) -> {
            SuppressionFactor suppressionFactor = addTrafficData(
                    info.getClientId(),
                    new TopicLoadInfo(info.getClientId(), info.getFrom(), info.getTo(), trafficData)
            );
            log.debug("Topic: {}, SF thr-pt: {}", trafficData.getTopic(), suppressionFactor.getThroughputFactor());
            suppressionData.getSuppressionFactor().put(trafficData.getTopic(), suppressionFactor);
        });
        return suppressionData;
    }
}
