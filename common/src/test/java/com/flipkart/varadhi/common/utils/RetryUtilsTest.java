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
import static org.junit.jupiter.api.Assertions.assertFalse;
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

        CompletableFuture<Optional<Long>> future = RetryUtils.getAsync(scheduler, 3, 5L, Optional::isEmpty, () -> {
            attempts.getAndIncrement();
            return Optional.empty();
        });

        try {
            Optional<Long> result = future.get(2, TimeUnit.SECONDS);
            assertEquals(Optional.empty(), result);
        } catch (java.util.concurrent.ExecutionException e) {
            assertTrue(RetryUtils.isRetriesExceeded(e));
        }
        assertEquals(3, attempts.get());
    }

    @Test
    void getAsync_failsFastOnProbeException() throws Exception {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        AtomicInteger attempts = new AtomicInteger();

        CompletableFuture<Optional<Long>> future = RetryUtils.getAsync(
            scheduler,
            5,
            5L,
            Optional::isEmpty,
            () -> {
                attempts.getAndIncrement();
                throw new IllegalStateException("cache read failed");
            }
        );

        try {
            future.get(2, TimeUnit.SECONDS);
        } catch (java.util.concurrent.ExecutionException e) {
            assertFalse(RetryUtils.isRetriesExceeded(e));
            assertTrue(e.getCause() instanceof IllegalStateException);
        }
        assertEquals(1, attempts.get());
    }

    @Test
    void getAsync_reusesExecutorAcrossProbes() throws Exception {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        var executor = RetryUtils.<Optional<Long>>newResultPollingExecutor(
            scheduler,
            5,
            5L,
            Optional::isEmpty
        );
        AtomicInteger attempts = new AtomicInteger();

        CompletableFuture<Optional<Long>> first = RetryUtils.getAsync(
            executor,
            () -> attempts.incrementAndGet() >= 2 ? Optional.of(1L) : Optional.empty()
        );
        assertEquals(Optional.of(1L), first.get(2, TimeUnit.SECONDS));

        CompletableFuture<Optional<Long>> second = RetryUtils.getAsync(
            executor,
            () -> attempts.incrementAndGet() >= 4 ? Optional.of(2L) : Optional.empty()
        );
        assertEquals(Optional.of(2L), second.get(2, TimeUnit.SECONDS));
        assertTrue(attempts.get() >= 4);
    }
}
