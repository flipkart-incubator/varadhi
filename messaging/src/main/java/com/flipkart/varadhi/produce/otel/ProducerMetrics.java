package com.flipkart.varadhi.produce.otel;

import com.flipkart.varadhi.entities.ProduceContext;

public interface ProducerMetrics {
    void onMessageProduced(boolean succeeded, long producerLatency, ProduceContext context);
}
