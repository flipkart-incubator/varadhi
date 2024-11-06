package com.flipkart.varadhi.verticles.webserver;

import com.flipkart.varadhi.qos.DistributedRateLimiter;
import com.flipkart.varadhi.qos.FactorRateLimiter;
import com.flipkart.varadhi.qos.RateLimiter;
import com.flipkart.varadhi.qos.TopicRateLimiter;
import com.flipkart.varadhi.qos.entity.RateLimiterType;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
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
    private final Counter.Builder acceptedThrptCounter;
    private final Counter.Builder rejectedThrptCounter;
    private final Counter.Builder acceptedQPSCounter;
    private final Counter.Builder rejectedQPSCounter;
    private final MeterRegistry meterRegistry;
    private final String clientId;


    public RateLimiterService(DistributedRateLimiter distributedRateLimiter, MeterRegistry meterRegistry, int frequency, String clientId)
            throws UnknownHostException {
        topicRateLimiters = new HashMap<>();
        trafficAggregator = new TrafficAggregator(
                clientId,
                frequency,
                distributedRateLimiter,
                this,
                Executors.newScheduledThreadPool(1)
        );
        this.meterRegistry = meterRegistry;
        this.clientId = clientId;
        this.acceptedThrptCounter = Counter.builder("varadhi.producer.qos.allowedBytes");
        this.rejectedThrptCounter = Counter.builder("varadhi.producer.qos.rateLimitedBytes");
        this.acceptedQPSCounter = Counter.builder("varadhi.producer.qos.allowedQueries");
        this.rejectedQPSCounter = Counter.builder("varadhi.producer.qos.rateLimitedQueries");
    }

    private List<FactorRateLimiter> getRateLimiter(String topic) {
        if (!topicRateLimiters.containsKey(topic)) {
            topicRateLimiters.put(
                    topic,
                    List.of(new TopicRateLimiter(topic, RateLimiterType.THROUGHPUT))
            );
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
                    // TODO(rl): gauge on suppressionFactor
                    registerGauges();
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
                acceptedThrptCounter.tag("topic", topic).tag("client", this.clientId).register(meterRegistry).increment(dataSize);
                acceptedQPSCounter.tag("topic", topic).tag("client", this.clientId).register(meterRegistry).increment();
            } else {
                rejectedThrptCounter.tag("topic", topic).tag("client", this.clientId).register(meterRegistry).increment(dataSize);
                rejectedQPSCounter.tag("topic", topic).tag("client", this.clientId).register(meterRegistry).increment();
            }
            return allowed;
        });
    }

    // TODO(rl): validate if gauge can be registered again to replace the existing one
    private void registerGauges() {
        topicRateLimiters.forEach((topic, rateLimiters) -> {
            for (RateLimiter rateLimiter : rateLimiters) {
                if (rateLimiter instanceof TopicRateLimiter topicRateLimiter) {
                    Gauge.builder("varadhi.producer.qos.suppressionFactor", topicRateLimiter, TopicRateLimiter::getSuppressionFactor)
                            .tag("topic", topic)
                            .tag("client", this.clientId)
                            .register(meterRegistry);
                }
            }
        });
    }

}
