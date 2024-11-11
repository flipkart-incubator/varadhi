package com.flipkart.varadhi.verticles.webserver;

import com.flipkart.varadhi.qos.DistributedRateLimiter;
import com.flipkart.varadhi.qos.entity.ClientLoadInfo;
import com.flipkart.varadhi.qos.entity.SuppressionData;
import com.flipkart.varadhi.qos.entity.TrafficData;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * This class will capture incoming traffic usage by providing a method to add topic usage by producers.
 * This class will accumualte everything and send to the controller for further processing for rate limit.
 * Post that, reset the usage for the topic.
 */
@Slf4j
public class TrafficAggregator {

    private final ClientLoadInfo loadInfo;
    private final int frequency;
    private final ScheduledExecutorService scheduledExecutorService;
    private final DistributedRateLimiter distributedRateLimiter;
    private final RateLimiterService rateLimiterService;
    private final Map<String, ConcurrentTopicData> topicTrafficMap;

    // Inner class to aggregate topic data before sending to controller
    static class ConcurrentTopicData {
        private final String topic;
        private final LongAdder bytesIn;
        private final LongAdder rateIn;

        public ConcurrentTopicData(String topic) {
            this.topic = topic;
            this.bytesIn = new LongAdder();
            this.rateIn = new LongAdder();
        }
    }

    public TrafficAggregator(
            String clientId, int frequency, DistributedRateLimiter distributedRateLimiter, RateLimiterService rateLimiterService,
            ScheduledExecutorService scheduledExecutorService
    ) {
        this.frequency = frequency;
        this.scheduledExecutorService = scheduledExecutorService;
        this.distributedRateLimiter = distributedRateLimiter;
        this.rateLimiterService = rateLimiterService;
        this.loadInfo = new ClientLoadInfo(clientId, 0,0, new ArrayList<>());
        this.topicTrafficMap = new ConcurrentHashMap<>();
        sendUsageToController();
    }

    public void addTopicUsage(String topic, long dataSize, long queries) {
        topicTrafficMap.compute(topic, (k, v) -> {
            if (v == null) {
                v = new ConcurrentTopicData(topic);
            }
            v.rateIn.add(queries);
            v.bytesIn.add(dataSize);
            return v;
        });
    }

    // Overloaded method to add topic usage for single request
    public void addTopicUsage(String topic, long dataSize) {
        addTopicUsage(topic, dataSize, 1);
    }

    private void sendUsageToController() {
        // todo(rl): explore scheduleWithFixedDelay
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                sendTrafficUsageAndUpdateSuppressionFactor();
            } catch (Exception e) {
                log.error("Error while sending usage to controller", e);
            }
        }, frequency, frequency, TimeUnit.SECONDS);

    }

    private void sendTrafficUsageAndUpdateSuppressionFactor() {
        long currentTime = System.currentTimeMillis();
        loadInfo.setTo(currentTime);
        // convert ConcurrentTopicData to TrafficData.list
        topicTrafficMap.forEach((topic, data) -> loadInfo.getTopicUsageList()
                .add(new TrafficData(topic, data.bytesIn.sum(), data.rateIn.sum())));
        log.debug("Sending traffic data to controller: {}", loadInfo);
        // TODO(rl); simulate add delay for degradation;
        SuppressionData suppressionData = distributedRateLimiter.addTrafficData(loadInfo);
        applySuppressionFactors(suppressionData);
        resetData(currentTime);
    }

    private void applySuppressionFactors(SuppressionData suppressionData) {
        suppressionData.getSuppressionFactor().forEach(
                (topic, suppressionFactor) -> rateLimiterService.updateSuppressionFactor(topic,
                        suppressionFactor.getThroughputFactor()
                ));
    }

    private void resetData(long time) {
        loadInfo.setFrom(time);
        // remove snapshot from current aggregated data
        loadInfo.getTopicUsageList().forEach(trafficData -> {
            topicTrafficMap.get(trafficData.topic()).bytesIn.add(-trafficData.bytesIn());
            topicTrafficMap.get(trafficData.topic()).rateIn.add(-trafficData.rateIn());
        });
        // reset snapshot
        loadInfo.getTopicUsageList().clear();
    }

}
