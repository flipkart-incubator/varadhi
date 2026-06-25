package com.flipkart.varadhi.common.utils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RetryUtilsTest {

    private ScheduledExecutorService scheduler;

    @AfterEach
    void tearDown() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    @Test
    void getAsync_completesWhenProbeStopsRetrying() throws Exception {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        AtomicInteger attempts = new AtomicInteger();

        CompletableFuture<Optional<Long>> future = RetryUtils.getAsync(
            scheduler,
            5,
            5L,
            Optional::isEmpty,
            () -> attempts.incrementAndGet() >= 2 ? Optional.of(42L) : Optional.empty()
        );

        Optional<Long> result = future.get(2, TimeUnit.SECONDS);
        assertEquals(Optional.of(42L), result);
        assertTrue(attempts.get() >= 2);
    }

    @Test
    void getAsync_stopsAfterMaxAttempts() throws Exception {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        AtomicInteger attempts = new AtomicInteger();

        CompletableFuture<Optional<Long>> future = RetryUtils.getAsync(
            scheduler,
            3,
            5L,
            Optional::isEmpty,
            () -> {
                attempts.getAndIncrement();
                return Optional.empty();
            }
        );

        try {
            Optional<Long> result = future.get(2, TimeUnit.SECONDS);
            assertEquals(Optional.empty(), result);
        } catch (java.util.concurrent.ExecutionException e) {
            assertTrue(RetryUtils.isRetriesExceeded(e));
        }
        assertEquals(3, attempts.get());
    }
}
