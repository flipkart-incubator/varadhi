package com.flipkart.varadhi.produce.otel;

import io.micrometer.core.instrument.MeterRegistry;

import java.util.Map;

public class ProducerMetricHandler {
    private final boolean isMetricEnabled;
    private final MeterRegistry meterRegistry;

    public ProducerMetricHandler(boolean isMetricEnabled, MeterRegistry meterRegistry) {
        this.isMetricEnabled = isMetricEnabled;
        this.meterRegistry = meterRegistry;
    }


    public ProducerMetricsEmitter getEmitter(int messageSize, Map<String, String> produceAttributes) {
        if (isMetricEnabled) {
            return new ProducerMetricsEmitterImpl(meterRegistry, messageSize, produceAttributes);
        } else {
            return new ProducerMetricsEmitterNoOpImpl();
        }
    }
}
