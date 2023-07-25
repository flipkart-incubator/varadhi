package com.flipkart.varadhi.otel;

import com.flipkart.varadhi.entities.ProduceContext;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.Map;

public class ProduceMetricProvider {
    private final MeterRegistry meterRegistry;

    public ProduceMetricProvider(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        //TODO::Discuss should this be made optional (config driven)
        //TODO::Add required meters for metrics.
    }

    public void onProduceCompleted(
            long produceStartedTime,
            long produceCompletedTime,
            ProduceContext context,
            Map<String, String> tags
    ) {
    }

    //TODO::Add failure kind here.
    public void onProduceFailed(
            long produceStartedTime,
            long produceCompletedTime,
            ProduceContext context,
            Map<String, String> tags
    ) {
    }
}
