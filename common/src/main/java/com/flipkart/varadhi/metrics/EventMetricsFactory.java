package com.flipkart.varadhi.metrics;

import com.flipkart.varadhi.entities.auth.ResourceType;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.Objects;
import java.util.Set;

/**
 * Factory for creating event metric emitters.
 * <p>
 * Provides a centralized way to create metric emitters based on configuration.
 * Ensures proper initialization and resource management of metrics.
 * <p>
 * Usage example:
 * <pre>{@code
 * var config = new EventMetricsFactory.MetricsConfig(true, registry, supportedTypes);
 * try (var metrics = EventMetricsFactory.create(config)) {
 *     // Use metrics
 * }
 * }</pre>
 *
 * @see EventMetricsEmitter
 * @see EventMetrics
 */
public final class EventMetricsFactory {
    private EventMetricsFactory() {
        throw new AssertionError("No instances allowed");
    }

    /**
     * Creates an appropriate metrics emitter based on configuration.
     *
     * @param config Configuration for metrics initialization
     * @return An EventMetricsEmitter instance
     * @throws NullPointerException if config is null
     */
    public static EventMetricsEmitter create(MetricsConfig config) {
        Objects.requireNonNull(config, "config cannot be null");
        return config.enabled() ?
            new EventMetrics(config.registry(), config.supportedTypes()) :
            NoOpEventMetricsEmitter.INSTANCE;
    }

    /**
     * Configuration for event metrics initialization.
     * <p>
     * This record ensures immutability and proper validation of metrics configuration.
     *
     * @param enabled        Whether metrics are enabled
     * @param registry       The metrics registry to use
     * @param supportedTypes Set of resource types to track
     */
    public record MetricsConfig(boolean enabled, MeterRegistry registry, Set<ResourceType> supportedTypes) {
        /**
         * Creates a new metrics configuration with validation.
         *
         * @throws NullPointerException if registry or supportedTypes is null when metrics are enabled
         */
        public MetricsConfig {
            if (enabled) {
                Objects.requireNonNull(registry, "registry cannot be null when metrics are enabled");
                Objects.requireNonNull(supportedTypes, "supportedTypes cannot be null when metrics are enabled");
                supportedTypes = Set.copyOf(supportedTypes); // Defensive copy
            }
        }
    }
}
