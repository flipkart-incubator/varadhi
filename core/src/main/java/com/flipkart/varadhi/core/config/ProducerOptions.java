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

    /**
     * Approximate upper bound (ms) a pod waits for its TopicCache to reach the coordinated topic
     * version during a topic-transition stage before acking failure.
     */
    @Builder.Default
    private long transitionVersionWaitMs = 1000;

    /**
     * Delay (ms) between TopicCache version checks during a topic-transition stage. {@code 0} =
     * no delay (immediate retry); attempt budget still respects {@link #transitionVersionWaitMs}.
     */
    @Builder.Default
    private long transitionPollIntervalMs = 0;

    public static ProducerOptions defaultOptions() {
        return ProducerOptions.builder().build();
    }
}
