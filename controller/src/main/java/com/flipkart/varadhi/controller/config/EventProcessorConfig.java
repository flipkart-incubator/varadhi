package com.flipkart.varadhi.controller.config;

import java.time.Duration;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * Configuration for the EventProcessor component.
 * <p>
 * This class provides configuration parameters for controlling the behavior of the
 * EventProcessor, including timeouts, retry policies, and thread naming.
 * <p>
 * It uses the builder pattern for easy construction and provides sensible defaults
 * for all parameters.
 */
@Data
@Builder
@Slf4j
public class EventProcessorConfig {

    // Default timeout values
    private static final Duration DEFAULT_CLUSTER_MEMBER_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration DEFAULT_INITIAL_RETRY_DELAY = Duration.ofMillis(100);
    private static final Duration DEFAULT_MAX_BACKOFF_MS = Duration.ofMillis(30000); // 30 seconds
    private static final long DEFAULT_TASK_JOIN_TIMEOUT_MS = 5000; // 5 seconds

    private final Duration clusterMemberTimeout;
    private final Duration retryBackoff;
    private final Duration maxRetryBackoff;
    private final long tasksJoinTimeoutMs;

    /**
     * Creates a new EventProcessorConfig with default values.
     *
     * @return a new EventProcessorConfig with default values
     */
    public static EventProcessorConfig getDefault() {
        return EventProcessorConfig.builder()
                .clusterMemberTimeout(DEFAULT_CLUSTER_MEMBER_TIMEOUT)
                .retryBackoff(DEFAULT_INITIAL_RETRY_DELAY)
                .maxRetryBackoff(DEFAULT_MAX_BACKOFF_MS)
                .tasksJoinTimeoutMs(DEFAULT_TASK_JOIN_TIMEOUT_MS)
                .build();
    }
}
