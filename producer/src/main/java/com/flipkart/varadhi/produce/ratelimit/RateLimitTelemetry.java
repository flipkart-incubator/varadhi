package com.flipkart.varadhi.produce.ratelimit;

import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.produce.telemetry.ProducerMetrics;

import java.util.function.Function;

/** Records shadow-mode rate-limit rejections into per-topic {@link ProducerMetrics}. */
public final class RateLimitTelemetry {

    private final Function<String, ProducerMetrics> metricsProvider;

    public RateLimitTelemetry(Function<String, ProducerMetrics> metricsProvider) {
        this.metricsProvider = metricsProvider;
    }

    public void shadowRejected(VaradhiTopic topic, long messageBytes) {
        metricsProvider.apply(topic.getName()).shadowRejected(messageBytes);
    }
}
