package com.flipkart.varadhi.core.config;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProducerOptions {

    @Builder.Default
    private long producerCacheTtlSeconds = 3600;

    @Builder.Default
    private MetricsOptions metricsOptions = MetricsOptions.getDefault();

    public static ProducerOptions defaultOptions() {
        return ProducerOptions.builder().build();
    }

    /**
     * Configuration for producer metrics collection and reporting.
     * This class defines settings for latency percentiles, histogram generation,
     * and throughput calculation intervals.
     *
     * to configure metrics collection behavior.</p>
     */
    @Data
    @Builder
    public static class MetricsOptions {

        /**
         * Array of percentile values for latency tracking.
         * Values should be between 0.0 and 1.0.
         * Common percentiles are: 0.5 (median), 0.75, 0.90, 0.95, 0.99, 0.999
         */
        @NotNull
        @Size (min = 1, message = "At least one percentile value is required")
        @Builder.Default
        private double[] latencyPercentiles = {0.99, 0.999};

        public static MetricsOptions getDefault() {
            return MetricsOptions.builder().build();
        }
    }
}
