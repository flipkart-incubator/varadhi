package com.flipkart.varadhi.qos;

import com.flipkart.varadhi.qos.entity.RateLimiterType;
import lombok.extern.slf4j.Slf4j;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

@Slf4j
public class RateLimiterService {// todo(rl): should be an interface for update sf

    /**
     * Map of topic to rate limiters (different rate limiters for QPS, Throughput etc)
     */
    private final Map<String, List<FactorRateLimiter>> topicRateLimiters;
    private final TrafficAggregator trafficAggregator;
    private final RateLimiterMetrics rateLimiterMetrics;

    public RateLimiterService(
            DistributedRateLimiter distributedRateLimiter, RateLimiterMetrics rateLimiterMetrics, int frequency,
            String clientId
    )
            throws UnknownHostException {
        topicRateLimiters = new HashMap<>();
        trafficAggregator = new TrafficAggregator(
                clientId,
                frequency,
                distributedRateLimiter,
                this,
                Executors.newScheduledThreadPool(1)
        );
        this.rateLimiterMetrics = rateLimiterMetrics;
    }

    private List<FactorRateLimiter> getRateLimiter(String topic) {
        if (!topicRateLimiters.containsKey(topic)) {
            List<FactorRateLimiter> rateLimiters = List.of(new TopicRateLimiter(topic, RateLimiterType.THROUGHPUT));
            topicRateLimiters.put(
                    topic,
                    rateLimiters
            );
            // register all the topic rate limiter to observe rate limit factors
            rateLimiters.forEach(rl -> {
                if (rl instanceof TopicRateLimiter trl) {
                    registerGauges(topic, trl);
                }
            });
        }
        return topicRateLimiters.get(topic);
    }

    public void updateSuppressionFactor(String topic, Double suppressionFactor) {
        log.debug("Updating suppression factor for topic: {}", topic);
        getRateLimiter(topic).forEach(rl -> {
            if (rl instanceof TopicRateLimiter trl) {
                if (trl.getTopic().equals(topic)) {
                    log.debug("Setting SF for topic: {}, factor: {}, rl: {}", topic, suppressionFactor, trl.getType());
                    rl.updateSuppressionFactor(suppressionFactor);
                }
            }
        });
    }

    public boolean isAllowed(String topic, long dataSize) {
        trafficAggregator.addTopicUsage(topic, dataSize);
        // get all the rate limiters for given topic and check if all of them allow the request
        return getRateLimiter(topic).stream().allMatch(rl -> {
            boolean allowed = rl.addTrafficData(dataSize);
            if (allowed) {
                rateLimiterMetrics.addSuccessRequest(topic, dataSize);
            } else {
                rateLimiterMetrics.addRateLimitedRequest(topic, dataSize);
            }
            return allowed;
        });
    }

    private void registerGauges(String topic, TopicRateLimiter topicRateLimiter) {
        rateLimiterMetrics.registerSuppressionFactorGauge(topic, topicRateLimiter);
    }

}
