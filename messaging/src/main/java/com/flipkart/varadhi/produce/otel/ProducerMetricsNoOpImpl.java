package com.flipkart.varadhi.produce.otel;

import com.flipkart.varadhi.entities.ProduceContext;

public class ProducerMetricsNoOpImpl implements ProducerMetrics {
    @Override
    public void onMessageProduced(boolean succeeded, long producerLatency, ProduceContext context) {
        // do nothing.
    }
}
