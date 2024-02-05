package com.flipkart.varadhi.produce.otel;

import com.flipkart.varadhi.core.entities.ApiContext;

public class ProducerMetricsNoOpImpl implements ProducerMetrics {
    @Override
    public void onMessageProduced(boolean succeeded, long producerLatency, ApiContext context) {
        // do nothing.
    }
}
