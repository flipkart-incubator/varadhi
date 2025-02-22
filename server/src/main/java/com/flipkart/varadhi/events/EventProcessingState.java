package com.flipkart.varadhi.events;

import com.flipkart.varadhi.entities.ResourceEvent;
import lombok.Getter;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

/**
 * Represents the state of an event being processed in the system.
 * This class tracks processing attempts, errors, completion status and provides
 * a future for async completion handling.
 */
@Getter
public final class EventProcessingState {
    private final ResourceEvent event;
    private final Instant startTime;
    private final CompletableFuture<Void> future;
    private int attempts;
    private Throwable lastError;
    private volatile boolean completed;

    /**
     * Creates a new EventProcessingState for the given event.
     *
     * @param event The resource event to be processed
     */
    public EventProcessingState(ResourceEvent event) {
        this.event = event;
        this.startTime = Instant.now();
        this.attempts = 0;
        this.future = new CompletableFuture<>();
    }

    /**
     * Increments the number of processing attempts.
     */
    void incrementAttempts() {
        attempts++;
    }

    /**
     * Marks the processing as failed with the given error.
     *
     * @param error The error that caused the failure
     */
    void fail(Throwable error) {
        lastError = error;
        future.completeExceptionally(error);
    }

    /**
     * Marks the processing as successfully completed.
     */
    void complete() {
        completed = true;
        future.complete(null);
    }

    /**
     * Gets the duration in milliseconds since processing started.
     *
     * @return Processing duration in milliseconds
     */
    long getDuration() {
        return Instant.now().toEpochMilli() - startTime.toEpochMilli();
    }
}
