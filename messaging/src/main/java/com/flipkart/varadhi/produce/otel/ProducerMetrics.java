package com.flipkart.varadhi.produce.otel;

import com.flipkart.varadhi.core.entities.ApiContext;

public interface ProducerMetrics {
    void onMessageProduced(boolean succeeded, long producerLatency, ApiContext context);
}
