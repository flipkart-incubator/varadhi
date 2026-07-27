package com.flipkart.varadhi.core.config;

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

    /** Pod-side topic-transition stage wait tuning; see {@link ProduceTransitionOptions}. */
    @Builder.Default
    private ProduceTransitionOptions transition = ProduceTransitionOptions.defaultOptions();

    public static ProducerOptions defaultOptions() {
        return ProducerOptions.builder().build();
    }
}
