package com.flipkart.varadhi.common.utils;

import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.FailsafeExecutor;
import dev.failsafe.RetryPolicy;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Minimal retry helpers for result-based async polling (e.g. TopicCache version convergence).
 */
public final class RetryUtils {

    private RetryUtils() {
    }

    /**
     * Reusable executor for result polling. Create once per handler; call {@link #getAsync(Supplier)}
     * per probe.
     */
    public static final class ResultPollingExecutor<T> {
        private final FailsafeExecutor<T> executor;

        private ResultPollingExecutor(FailsafeExecutor<T> executor) {
            this.executor = executor;
        }

        public CompletableFuture<T> getAsync(Supplier<T> probe) {
            return executor.getAsync(probe::get);
        }
    }

    /**
     * Builds a reusable executor for result polling: retries only while {@code retryOnResult}
     * matches; probe exceptions abort immediately.
     */
    public static <T> ResultPollingExecutor<T> newResultPollingExecutor(
        Executor executor,
        int maxAttempts,
        long delayInMs,
        Predicate<T> retryOnResult
    ) {
        RetryPolicy<T> policy = RetryPolicy.<T>builder()
                                           .withMaxAttempts(maxAttempts)
                                           .withDelay(Duration.ofMillis(delayInMs))
                                           .handleResultIf(retryOnResult::test)
                                           .abortOn(Exception.class)
                                           .build();
        return new ResultPollingExecutor<>(Failsafe.with(policy).with(executor));
    }

    public static <T> CompletableFuture<T> getAsync(ResultPollingExecutor<T> executor, Supplier<T> probe) {
        return executor.getAsync(probe);
    }

    public static <T> CompletableFuture<T> getAsync(
        Executor executor,
        int maxAttempts,
        long delayInMs,
        Predicate<T> retryOnResult,
        Supplier<T> probe
    ) {
        return getAsync(newResultPollingExecutor(executor, maxAttempts, delayInMs, retryOnResult), probe);
    }

    /**
     * Returns true when {@code t} (or its cause) is a {@link FailsafeException} from
     * result-polling exhaustion. Probe exceptions are not retried ({@code abortOn(Exception)}),
     * so they surface as their original type instead.
     */
    public static boolean isRetriesExceeded(Throwable t) {
        return ThrowableUtils.unwrap(t) instanceof FailsafeException;
    }
}
