package com.flipkart.varadhi.produce.config;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ProducerOptions {

    @Builder.Default
    private long producerCacheTtlSeconds = 3600;

    @Builder.Default
    private boolean metricEnabled = true;

    public static ProducerOptions defaultOptions() {
        return ProducerOptions.builder().build();
    }
}
