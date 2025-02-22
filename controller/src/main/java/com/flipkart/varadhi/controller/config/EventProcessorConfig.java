package com.flipkart.varadhi.controller.config;

import lombok.Builder;
import lombok.Getter;

import java.time.Duration;

/**
 * Configuration for event processing in a distributed system.
 * Provides settings for retry behavior, timeouts, and concurrency control.
 */
@Getter
@Builder
public class EventProcessorConfig {
    private final int maxRetries; // Maximum number of retry attempts for failed events
    private final long retryDelayMs; // Base delay between retry attempts
    private final long clusterMemberTimeoutMs; // Maximum time to wait for cluster member response
    private final int maxConcurrentProcessing; // Maximum number of events processed concurrently
    private final Duration processingTimeout; // Maximum time allowed for complete event processing

    /**
     * Creates a default configuration with reasonable values for most use cases.
     *
     * @return A new EventProcessorConfig instance with default values
     */
    public static EventProcessorConfig getDefault() {
        return EventProcessorConfig.builder()
                .maxRetries(3)
                .retryDelayMs(1000)
                .clusterMemberTimeoutMs(5000)
                .maxConcurrentProcessing(100)
                .processingTimeout(Duration.ofMinutes(5))
                .build();
    }
}
