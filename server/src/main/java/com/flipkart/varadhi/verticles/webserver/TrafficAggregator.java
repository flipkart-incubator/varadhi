package com.flipkart.varadhi.verticles.webserver;

import com.flipkart.varadhi.cluster.MessageExchange;
import com.flipkart.varadhi.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.qos.entity.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import java.util.concurrent.atomic.LongAdder;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.flipkart.varadhi.core.cluster.ControllerApi.ROUTE_CONTROLLER;

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
    private final MessageExchange exchange;
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
            String clientId, int frequency, MessageExchange exchange, RateLimiterService rateLimiterService,
            ScheduledExecutorService scheduledExecutorService
    ) {
        this.frequency = frequency;
        this.scheduledExecutorService = scheduledExecutorService;
        this.exchange = exchange;
        this.rateLimiterService = rateLimiterService;
        this.loadInfo = new ClientLoadInfo(clientId, 0,0, new ArrayList<>());
        this.topicTrafficMap = new HashMap<>();
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
        topicTrafficMap.forEach((topic, data) -> {
            loadInfo.getTopicUsageList().add(TrafficData.builder().topic(topic).bytesIn(data.bytesIn.sum()).rateIn(data.rateIn.sum()).build());
        });
        // TODO(rl): use load info
        ClusterMessage msg = ClusterMessage.of(loadInfo);
        log.info("Sending traffic data to controller: {}", loadInfo);
        exchange.request(ROUTE_CONTROLLER, "collect", msg)
                .thenApply(rm -> rm.getResponse(SuppressionData.class))
                .whenComplete(this::handleSuppressionDataResponse);// TODO(rl); simulate add delay for degradation
        resetData(currentTime);// TODO(rl): same time as to in from
    }

    private void handleSuppressionDataResponse(
            SuppressionData suppressionData, Throwable throwable
    ) {
        if (throwable != null) {
            log.error("Error while receiving suppression data from controller", throwable);
        } else {
            suppressionData.getSuppressionFactor().forEach((topic, suppressionFactor) -> {
                rateLimiterService.updateSuppressionFactor(
                        topic, RateLimiterType.THROUGHPUT_CHECK, suppressionFactor.getThroughputFactor());
            });
        }
    }

    private void resetData(long time) {
        loadInfo.setFrom(time);
        // remove snapshot from current aggregated data
        loadInfo.getTopicUsageList().forEach(trafficData -> {
            topicTrafficMap.get(trafficData.getTopic()).bytesIn.add(-trafficData.getBytesIn());
            topicTrafficMap.get(trafficData.getTopic()).rateIn.add(-trafficData.getRateIn());
        });
        // reset snapshot
        loadInfo.getTopicUsageList().clear();
    }

}
