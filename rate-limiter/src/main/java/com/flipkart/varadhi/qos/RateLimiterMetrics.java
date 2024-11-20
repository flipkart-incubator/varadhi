package com.flipkart.varadhi.qos;

import io.micrometer.core.instrument.MeterRegistry;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RateLimiterMetrics {
    private final MeterRegistry meterRegistry;
    private final String clientId;
    private final Map<String, TopicMetricsHandler> topicMetricsHandlers;
    private final String acceptedThrptCounterName = "varadhi.producer.qos.allowedBytes";
    private final String rejectedThrptCounterName = "varadhi.producer.qos.rateLimitedBytes";
    private final String acceptedQPSCounterName = "varadhi.producer.qos.allowedQueries";
    private final String rejectedQPSCounterName = "varadhi.producer.qos.rateLimitedQueries";
    private final String suppressionFactorGaugeName = "varadhi.producer.qos.suppressionFactor";

    public RateLimiterMetrics(MeterRegistry meterRegistry, String clientId) {
        this.meterRegistry = meterRegistry;
        this.clientId = clientId;
        this.topicMetricsHandlers = new ConcurrentHashMap<>();
    }


    public void addSuccessRequest(String topic, long dataSize) {
        TopicMetricsHandler metricsHandler = getTopicMetricsHandler(topic);
        metricsHandler.getCounter(acceptedThrptCounterName).increment(dataSize);
        metricsHandler.getCounter(acceptedQPSCounterName).increment();
    }

    public void addRateLimitedRequest(String topic, long dataSize) {
        TopicMetricsHandler metricsHandler = getTopicMetricsHandler(topic);
        metricsHandler.getCounter(rejectedThrptCounterName).increment(dataSize);
        metricsHandler.getCounter(rejectedQPSCounterName).increment();
    }

    private TopicMetricsHandler getTopicMetricsHandler(String topic) {
        return topicMetricsHandlers.compute(topic, (k, v) -> {
            if (v == null) {
                v = new TopicMetricsHandler(meterRegistry, clientId, topic);
            }
            return v;
        });
    }

    public void registerSuppressionFactorGauge(String topic, TopicRateLimiter topicRateLimiter) {
        TopicMetricsHandler metricsHandler = getTopicMetricsHandler(topic);
        metricsHandler.getTopicRateLimiterGauge(suppressionFactorGaugeName, topicRateLimiter);
    }

}
