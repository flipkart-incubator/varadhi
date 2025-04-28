package com.flipkart.varadhi.web.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Handler for creating HTTP API metrics emitters.
 * Similar to ProducerMetricHandler, this class acts as a factory for creating
 * appropriate metric emitters based on whether metrics are enabled.
 */
public final class HttpApiMetricsHandler {
    private final boolean isMetricEnabled;
    private final MeterRegistry meterRegistry;

    /**
     * Constructs a new HttpApiMetricsHandler.
     *
     * @param isMetricEnabled whether metrics collection is enabled
     * @param meterRegistry   the registry for recording metrics
     */
    public HttpApiMetricsHandler(boolean isMetricEnabled, MeterRegistry meterRegistry) {
        this.isMetricEnabled = isMetricEnabled;
        this.meterRegistry = meterRegistry;
    }

    /**
     * Creates a metric emitter for a specific API endpoint.
     *
     * @param apiName the name of the API endpoint
     * @return an HttpApiMetricsEmitter instance
     */
    public HttpApiMetricsEmitter getEmitter(String apiName) {
        return getEmitter(apiName, List.of());
    }

    /**
     * Creates a metric emitter for a specific API endpoint with additional tags.
     *
     * @param apiName the name of the API endpoint
     * @param tags    additional tags to include with all metrics
     * @return an HttpApiMetricsEmitter instance
     */
    public HttpApiMetricsEmitter getEmitter(String apiName, List<Tag> tags) {
        return isMetricEnabled ?
            new HttpApiMetricsEmitterImpl(meterRegistry, apiName, tags) :
            new HttpApiMetricsEmitterNoOpImpl();
    }

    /**
     * Creates a metric emitter for a specific API endpoint with attributes as tags.
     *
     * @param apiName     the name of the API endpoint
     * @param attributes  a map of attribute names to values to include as tags
     * @return an HttpApiMetricsEmitter instance
     */
    public HttpApiMetricsEmitter getEmitter(String apiName, Map<String, String> attributes) {
        List<Tag> tags = attributes.entrySet()
                                   .stream()
                                   .map(entry -> Tag.of(entry.getKey(), entry.getValue()))
                                   .collect(Collectors.toList());
        return getEmitter(apiName, tags);
    }
}
