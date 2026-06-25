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
     * Max time (ms) a pod waits for its TopicCache to reach the coordinated topic version
     * during a topic-transition stage (PREPARE/SWITCH) before acking failure.
     */
    @Builder.Default
    private long transitionVersionWaitMs = 5000;

    /** Poll interval (ms) between TopicCache version checks during a topic-transition stage. */
    @Builder.Default
    private long transitionPollIntervalMs = 25;

    public static ProducerOptions defaultOptions() {
        return ProducerOptions.builder().build();
    }
}
