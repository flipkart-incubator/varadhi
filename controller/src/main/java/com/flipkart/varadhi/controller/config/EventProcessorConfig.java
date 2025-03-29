package com.flipkart.varadhi.controller.config;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;

@Data
@Builder
@Slf4j
public class EventProcessorConfig {

    private static final Duration DEFAULT_SHUTDOWN_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration DEFAULT_INITIAL_RETRY_DELAY = Duration.ofMillis(100);
    private static final Duration DEFAULT_MAX_RETRY_DELAY = Duration.ofSeconds(30);
    private static final Duration DEFAULT_CLUSTER_MEMBER_TIMEOUT = Duration.ofSeconds(10);
    private static final int DEFAULT_MAX_BACKOFF_ATTEMPTS = 10;
    private static final long DEFAULT_MAX_BACKOFF_MS = 30000; // 30 seconds
    private static final long DEFAULT_EVENT_READY_CHECK_MS = 500; // 0.5 second
    private static final long DEFAULT_THREAD_JOIN_TIMEOUT_PRIMARY_MS = 5000; // 5 seconds
    private static final long DEFAULT_THREAD_JOIN_TIMEOUT_WORKER_MS = 1000; // 1 second

    private static final String DEFAULT_EVENT_MANAGER_THREAD_NAME = "event-manager";
    private static final String DEFAULT_EVENT_COMMITTER_THREAD_NAME = "event-committer";
    private static final String DEFAULT_NODE_PROCESSOR_THREAD_PREFIX = "node-processor-";

    private final Duration shutdownTimeout;
    private final Duration initialRetryDelay;
    private final Duration maxRetryDelay;
    private final String eventManagerThreadName;

    private final Duration clusterMemberTimeout;
    private final Duration retryDelay;
    private final int maxBackoffAttempts;
    private final long maxBackoffMs;
    private final long eventReadyCheckMs;
    private final long threadJoinTimeoutPrimaryMs;
    private final long threadJoinTimeoutWorkerMs;
    private final String eventCommitterThreadName;
    private final String nodeProcessorThreadPrefix;

    private final double minJitterFactor;
    private final double maxJitterFactor;

    public static EventProcessorConfig getDefault() {
        return EventProcessorConfig.builder()
                                   .shutdownTimeout(DEFAULT_SHUTDOWN_TIMEOUT)
                                   .initialRetryDelay(DEFAULT_INITIAL_RETRY_DELAY)
                                   .maxRetryDelay(DEFAULT_MAX_RETRY_DELAY)
                                   .eventManagerThreadName(DEFAULT_EVENT_MANAGER_THREAD_NAME)

                                   .clusterMemberTimeout(DEFAULT_CLUSTER_MEMBER_TIMEOUT)
                                   .retryDelay(DEFAULT_INITIAL_RETRY_DELAY)
                                   .maxBackoffAttempts(DEFAULT_MAX_BACKOFF_ATTEMPTS)
                                   .maxBackoffMs(DEFAULT_MAX_BACKOFF_MS)
                                   .eventReadyCheckMs(DEFAULT_EVENT_READY_CHECK_MS)
                                   .threadJoinTimeoutPrimaryMs(DEFAULT_THREAD_JOIN_TIMEOUT_PRIMARY_MS)
                                   .threadJoinTimeoutWorkerMs(DEFAULT_THREAD_JOIN_TIMEOUT_WORKER_MS)
                                   .eventCommitterThreadName(DEFAULT_EVENT_COMMITTER_THREAD_NAME)
                                   .nodeProcessorThreadPrefix(DEFAULT_NODE_PROCESSOR_THREAD_PREFIX)

                                   .minJitterFactor(0.8)
                                   .maxJitterFactor(1.2)
                                   .build();
    }

    public long getRetryDelayMs() {
        return retryDelay.toMillis();
    }

    public long getClusterMemberTimeoutMs() {
        return clusterMemberTimeout.toMillis();
    }

    public long getShutdownTimeoutSeconds() {
        return shutdownTimeout.getSeconds();
    }

    public long calculateRetryDelayMs(int retryCount) {
        long baseDelay = Math.min(
            initialRetryDelay.toMillis() * (1L << Math.min(retryCount, maxBackoffAttempts)),
            maxRetryDelay.toMillis()
        );

        double jitter = minJitterFactor + Math.random() * (maxJitterFactor - minJitterFactor);
        return (long)(baseDelay * jitter);
    }

    public String getNodeProcessorThreadName(String hostname) {
        return nodeProcessorThreadPrefix + hostname;
    }
}
