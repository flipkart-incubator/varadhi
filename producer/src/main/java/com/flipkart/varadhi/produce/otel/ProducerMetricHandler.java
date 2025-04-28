package com.flipkart.varadhi.produce.otel;

import com.flipkart.varadhi.produce.config.ProducerMetricsConfig;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.Map;

/**
 * Handles the creation of metric emitters for producers based on configuration.
 * This class acts as a factory for creating appropriate metric emitters based on whether
 * metrics are enabled or disabled.
 *
 * @see ProducerMetricsEmitter
 * @see ProducerMetricsEmitterImpl
 * @see ProducerMetricsEmitterNoOpImpl
 */
public final class ProducerMetricHandler {
    private final boolean isMetricEnabled;
    private final MeterRegistry meterRegistry;
    private final ProducerMetricsConfig metricsConfig;

    /**
     * Constructs a new ProducerMetricHandler.
     *
     * @param isMetricEnabled whether metrics collection is enabled
     * @param meterRegistry   the registry for recording metrics
     * @param metricsConfig   configuration for producer metrics
     */
    public ProducerMetricHandler(
        boolean isMetricEnabled,
        MeterRegistry meterRegistry,
        ProducerMetricsConfig metricsConfig
    ) {
        this.isMetricEnabled = isMetricEnabled;
        this.meterRegistry = meterRegistry;
        this.metricsConfig = metricsConfig;
    }

    /**
     * Creates and returns a metrics emitter based on whether metrics are enabled.
     * If metrics are enabled, returns an implementation that records metrics.
     * If metrics are disabled, returns a no-op implementation.
     *
     * @param produceAttributes attributes to be included with the metrics
     * @return a ProducerMetricsEmitter instance
     */
    public ProducerMetricsEmitter getEmitter(Map<String, String> produceAttributes) {
        return isMetricEnabled ?
            new ProducerMetricsEmitterImpl(meterRegistry, metricsConfig, produceAttributes) :
            new ProducerMetricsEmitterNoOpImpl();
    }
}
