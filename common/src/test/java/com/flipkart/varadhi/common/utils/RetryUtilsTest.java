package com.flipkart.varadhi.common.utils;

import dev.failsafe.FailsafeExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
    void newPollingExecutor_completesWhenProbeStopsRetrying() throws Exception {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        FailsafeExecutor<Optional<Long>> executor = RetryUtils.newPollingExecutor(scheduler, 5, 5L, Optional::isEmpty);
        AtomicInteger attempts = new AtomicInteger();

        CompletableFuture<Optional<Long>> future = executor.getAsync(
            () -> attempts.incrementAndGet() >= 2 ? Optional.of(42L) : Optional.empty()
        );

        assertEquals(Optional.of(42L), future.get(2, TimeUnit.SECONDS));
        assertTrue(attempts.get() >= 2);
    }

    @Test
    void newPollingExecutor_stopsAfterMaxAttempts() {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        FailsafeExecutor<Optional<Long>> executor = RetryUtils.newPollingExecutor(scheduler, 3, 5L, Optional::isEmpty);
        AtomicInteger attempts = new AtomicInteger();

        CompletableFuture<Optional<Long>> future = executor.getAsync(() -> {
            attempts.getAndIncrement();
            return Optional.empty();
        });

        ExecutionException thrown = assertThrows(ExecutionException.class, () -> future.get(2, TimeUnit.SECONDS));
        assertInstanceOf(TimeoutException.class, thrown.getCause());
        assertEquals(3, attempts.get());
    }

    @Test
    void newPollingExecutor_abortsImmediatelyOnProbeException() {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        FailsafeExecutor<Optional<Long>> executor = RetryUtils.newPollingExecutor(scheduler, 5, 5L, Optional::isEmpty);
        AtomicInteger attempts = new AtomicInteger();

        CompletableFuture<Optional<Long>> future = executor.getAsync(() -> {
            attempts.getAndIncrement();
            throw new IllegalStateException("topic gone");
        });

        ExecutionException thrown = assertThrows(ExecutionException.class, () -> future.get(2, TimeUnit.SECONDS));
        assertInstanceOf(IllegalStateException.class, thrown.getCause());
        assertEquals("topic gone", thrown.getCause().getMessage());
        assertEquals(1, attempts.get());
    }
}
