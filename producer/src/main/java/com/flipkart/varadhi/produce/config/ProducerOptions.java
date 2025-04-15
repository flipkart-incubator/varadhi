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

    public static ProducerOptions defaultOptions() {
        return ProducerOptions.builder().build();
    }
}
