package com.flipkart.varadhi.produce.otel;

public interface ProducerMetricsEmitter {
    void emit(boolean succeeded, long producerLatency);
}
