package com.flipkart.varadhi.qos;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TopicMetricsHandler {
    private final MeterRegistry meterRegistry;
    private final String clientId;
    private final String topic;
    private final Map<String, Counter> counterCache;
    private final Map<String, Gauge> gaugeCache;

    public TopicMetricsHandler(MeterRegistry meterRegistry, String clientId, String topic) {
        this.meterRegistry = meterRegistry;
        this.clientId = clientId;
        this.topic = topic;
        this.counterCache = new ConcurrentHashMap<>();
        this.gaugeCache = new ConcurrentHashMap<>();
    }

    public Counter getCounter(String metricName) {
        return counterCache.compute(metricName, (k, v) -> {
            if (v == null) {
                v = Counter.builder(metricName)
                        .tag("topic", topic)
                        .tag("client", clientId)
                        .register(meterRegistry);
            }
            return v;
        });
    }

    public Gauge getTopicRateLimiterGauge(String metricName, TopicRateLimiter topicRateLimiter) {
        return gaugeCache.compute(metricName, (k, v) -> {
            if (v == null) {
                v = Gauge.builder(metricName, topicRateLimiter, TopicRateLimiter::getSuppressionFactor)
                        .tag("topic", topic)
                        .tag("client", clientId)
                        .register(meterRegistry);
            }
            return v;
        });
    }

}
