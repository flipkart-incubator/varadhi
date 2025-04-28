package com.flipkart.varadhi.produce.config;

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
    private boolean metricEnabled = true;

    @Builder.Default
    private ProducerMetricsConfig metricsConfig = ProducerMetricsConfig.getDefault();

    public static ProducerOptions defaultOptions() {
        return ProducerOptions.builder().build();
    }
}
