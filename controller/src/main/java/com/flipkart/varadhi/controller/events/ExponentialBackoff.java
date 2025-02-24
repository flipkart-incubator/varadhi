package com.flipkart.varadhi.controller.events;

import com.flipkart.varadhi.entities.ResourceEvent;
import com.flipkart.varadhi.exceptions.EventProcessingException;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;

/**
 * Implements exponential backoff retry logic for event publishing.
 * Manages retry attempts with increasing delays between attempts.
 *
 * <h2>Retry Behavior</h2>
 * <ul>
 *   <li>Delay increases exponentially with each attempt</li>
 *   <li>Tracks attempts against maximum allowed retries</li>
 *   <li>Provides backoff delays between retry attempts</li>
 *   <li>Handles interruption during backoff periods</li>
 * </ul>
 */
@Slf4j
public final class ExponentialBackoff {
    private final int maxRetries;
    private final Duration baseDelay;
    private int attempts = 0;

    /**
     * Creates a new ExponentialBackoff instance.
     *
     * @param maxRetries Maximum number of retry attempts allowed
     * @param baseDelay  Base delay duration between retries
     */
    public ExponentialBackoff(int maxRetries, Duration baseDelay) {
        this.maxRetries = maxRetries;
        this.baseDelay = baseDelay;
    }

    /**
     * Checks if additional retry attempts are available.
     *
     * @return true if more retries are allowed, false otherwise
     */
    public boolean shouldRetry() {
        return attempts < maxRetries;
    }

    /**
     * Handles a failure by incrementing attempts and potentially throwing an exception.
     * If max retries are exceeded, throws an EventProcessingException.
     *
     * @param e     The exception that caused the failure
     * @param event The event that failed to process
     * @throws EventProcessingException if maximum retries are exceeded
     */
    public void onFailure(Exception e, ResourceEvent event) {
        attempts++;
        if (!shouldRetry()) {
            throw new EventProcessingException(
                String.format("Failed to publish event after %d attempts: %s", maxRetries, event),
                e
            );
        }
        log.warn("Retry {} - Failed to publish event: {}", attempts, event, e);
        backoff();
    }

    /**
     * Implements the exponential backoff delay.
     * Delay duration increases with each attempt.
     *
     * @throws EventProcessingException if interrupted during backoff
     */
    private void backoff() {
        try {
            Thread.sleep(baseDelay.multipliedBy(attempts));
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new EventProcessingException("Interrupted during retry", ie);
        }
    }
}
