package com.flipkart.varadhi.produce.otel;

public class ProducerMetricsEmitterNoOpImpl implements ProducerMetricsEmitter {
    @Override
    public void emit(boolean succeeded, long producerLatency) {
        // do nothing.
    }
}
