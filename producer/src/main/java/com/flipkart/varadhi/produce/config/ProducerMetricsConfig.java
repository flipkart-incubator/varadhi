package com.flipkart.varadhi.produce.config;

import com.flipkart.varadhi.produce.otel.ProducerMetricsEmitterImpl;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import lombok.Builder;
import lombok.Data;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

/**
 * Configuration for producer metrics collection and reporting.
 * This class defines settings for latency percentiles, histogram generation,
 * and throughput calculation intervals.
 *
 * <p>Used by {@link ProducerMetricsEmitterImpl}
 * to configure metrics collection behavior.</p>
 *
 * @see ProducerMetricsEmitterImpl
 */
@Data
@Builder
public class ProducerMetricsConfig {

    /**
     * Array of percentile values for latency tracking.
     * Values should be between 0.0 and 1.0.
     * Common percentiles are: 0.5 (median), 0.75, 0.90, 0.95, 0.99, 0.999
     */
    @NotNull
    @Size(min = 1, message = "At least one percentile value is required")
    @Builder.Default
    private double[] latencyPercentiles = {0.5, 0.75, 0.90, 0.95, 0.99, 0.999};

    /**
     * Controls whether histogram metrics should be generated.
     * Histograms provide detailed distribution information but consume more memory.
     */
    @Builder.Default
    private boolean enableHistogram = true;

    /**
     * Interval at which throughput metrics are refreshed.
     * This affects the granularity of throughput measurements.
     */
    @NotNull
    @Positive(message = "Throughput refresh interval must be positive")
    @Builder.Default
    private Duration throughputRefreshInterval = Duration.ofSeconds(10);

    /**
     * List of attribute names to be included as metric tags.
     * If empty, all attributes will be included.
     */
    @Builder.Default
    private Set<String> includedTags = new HashSet<>();

    /**
     * Whether to include all attributes as tags when includedTags is empty.
     * If false and includedTags is empty, no attribute tags will be added.
     */
    @Builder.Default
    private boolean includeAllTagsWhenEmpty = false;

    /**
     * Creates a new instance with default values.
     *
     * @return a new ProducerMetricsConfig instance with default settings
     */
    public static ProducerMetricsConfig getDefault() {
        return ProducerMetricsConfig.builder().build();
    }
}
