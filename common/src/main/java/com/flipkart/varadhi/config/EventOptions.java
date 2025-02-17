package com.flipkart.varadhi.config;

import jakarta.validation.constraints.NotNull;

/**
 * Configuration options for event-related operations in Varadhi.
 * <p>
 * This record is part of the application configuration and controls event processing
 * behavior across the system. It is primarily used by WebServerVerticle for
 * initializing event-related services and metrics collection.
 * <p>
 * Example configuration:
 * <pre>
 * events:
 *   metricsEnabled: true
 * </pre>
 */
public record EventOptions(@NotNull boolean metricsEnabled) {
    /**
     * Default value for metrics enabled flag.
     */
    private static final boolean DEFAULT_METRICS_ENABLED = false;

    /**
     * Creates default event options with metrics disabled.
     *
     * @return A new EventOptions instance with default settings
     */
    public static EventOptions getDefault() {
        return new EventOptions(DEFAULT_METRICS_ENABLED);
    }
}
