package com.flipkart.varadhi.common.utils;

import dev.failsafe.FailsafeExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

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

class RetryUtilsTest {

    private ScheduledExecutorService scheduler;

    @AfterEach
    void tearDown() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    @Test
    void newPollingExecutor_retriesThenSucceeds() throws Exception {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        FailsafeExecutor<String> executor = RetryUtils.newPollingExecutor(
            scheduler,
            5,
            5L,
            IllegalStateException.class
        );
        AtomicInteger attempts = new AtomicInteger();

        CompletableFuture<String> future = executor.getAsync(() -> {
            if (attempts.incrementAndGet() < 3) {
                throw new IllegalStateException("not yet");
            }
            return "ok";
        });

        assertEquals("ok", future.get(2, TimeUnit.SECONDS));
        assertEquals(3, attempts.get());
    }

    @Test
    void newPollingExecutor_exhaustionIsTimeout() {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        FailsafeExecutor<String> executor = RetryUtils.newPollingExecutor(
            scheduler,
            3,
            5L,
            IllegalStateException.class
        );
        AtomicInteger attempts = new AtomicInteger();

        CompletableFuture<String> future = executor.getAsync(() -> {
            attempts.getAndIncrement();
            throw new IllegalStateException("not yet");
        });

        ExecutionException thrown = assertThrows(ExecutionException.class, () -> future.get(2, TimeUnit.SECONDS));
        assertInstanceOf(TimeoutException.class, thrown.getCause());
        assertEquals(3, attempts.get());
    }

    @Test
    void newPollingExecutor_nonRetryableFailsImmediately() {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        FailsafeExecutor<String> executor = RetryUtils.newPollingExecutor(
            scheduler,
            5,
            5L,
            IllegalStateException.class
        );
        AtomicInteger attempts = new AtomicInteger();

        CompletableFuture<String> future = executor.getAsync(() -> {
            attempts.getAndIncrement();
            throw new IllegalArgumentException("hard fail");
        });

        ExecutionException thrown = assertThrows(ExecutionException.class, () -> future.get(2, TimeUnit.SECONDS));
        assertInstanceOf(IllegalArgumentException.class, thrown.getCause());
        assertEquals(1, attempts.get());
    }
}
