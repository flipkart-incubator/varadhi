package com.flipkart.varadhi.qos.client;

import com.flipkart.varadhi.cluster.MessageExchange;
import com.flipkart.varadhi.qos.RateLimiter;
import com.flipkart.varadhi.qos.TopicRateLimiter;
import com.flipkart.varadhi.utils.HostUtils;

import com.flipkart.varadhi.verticles.webserver.TrafficAggregator;
import java.net.UnknownHostException;

import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class RateLimiterService {

    /**
     * Map of topic to rate limiters (different rate limiters for QPS, Throughput etc)
     */
    Map<String, List<RateLimiter>> topicRateLimiters;
    TrafficAggregator trafficAggregator;


    public RateLimiterService(MessageExchange exchange, int frequency, boolean requireHostName)
            throws UnknownHostException {
        topicRateLimiters = new HashMap<>();
        trafficAggregator = new TrafficAggregator(exchange, HostUtils.getHostNameOrAddress(requireHostName), frequency, this);
    }

    public List<RateLimiter> getRateLimiter(String topic) {
        if (!topicRateLimiters.containsKey(topic)) {
            topicRateLimiters.put(
                    topic,
                    List.of(
                            new TopicRateLimiter(topic, "throughput_check"),
                            new TopicRateLimiter(topic, "qps_check")
                    )
            );
        }
        return topicRateLimiters.get(topic);
    }

    public void updateSuppressionFactor(String topic, String type, Double suppressionFactor) {
        log.info("Updating suppression factor for topic: {}", topic);
        getRateLimiter(topic).forEach(rl -> {
            if(rl instanceof TopicRateLimiter trl) {
                if(trl.getTopic().equals(topic) && trl.getName().equals(type)) {
                    log.info("Setting SF for topic: {}, factor: {}, rl: {}", topic, suppressionFactor, trl.getName());
                    trl.setSuppressionFactor(suppressionFactor);
                }
            }
        });
    }

    public boolean isAllowed(String topic, Double throughput) {
        trafficAggregator.addTopicUsage(topic, throughput.longValue(), 1);
        return getRateLimiter(topic).stream().allMatch(rl -> {
            boolean allowed = rl.isAllowed(throughput);
            return allowed;
        });
    }



}
