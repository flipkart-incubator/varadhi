package com.flipkart.varadhi.common.utils;

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import dev.failsafe.RetryPolicyBuilder;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Minimal retry helpers for result-based async polling (e.g. TopicCache version convergence).
 *
 * <p>Only the methods required by topic-transition handling are included here; the broader oncall
 * {@code com.flipkart.varadhi.utils.RetryUtils} surface is not duplicated.
 */
public final class RetryUtils {

    private RetryUtils() {
    }

    /**
     * Runs {@code probe} on {@code executor}, retrying while {@code retryOnResult} matches, up to
     * {@code maxAttempts}. Intended for result-based polling; does not attach failure listeners so
     * expected “not yet ready” retries stay quiet.
     */
    public static <T> CompletableFuture<T> getAsync(
        Executor executor,
        int maxAttempts,
        long delayInMs,
        Predicate<T> retryOnResult,
        Supplier<T> probe
    ) {
        RetryPolicyBuilder<T> policy = RetryPolicy.<T>builder()
                                                  .withMaxAttempts(maxAttempts)
                                                  .withDelay(Duration.ofMillis(delayInMs))
                                                  .handleResultIf(retryOnResult::test);
        return Failsafe.with(policy.build()).with(executor).getAsync(probe::get);
    }

    /** Returns true when {@code t} (or its cause) is a Failsafe retries-exceeded failure. */
    public static boolean isRetriesExceeded(Throwable t) {
        return unwrap(t) instanceof dev.failsafe.FailsafeException;
    }

    public static String rootMessage(Throwable t) {
        Throwable cause = unwrap(t);
        return cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName();
    }

    private static Throwable unwrap(Throwable t) {
        if (t instanceof java.util.concurrent.CompletionException
            || t instanceof java.util.concurrent.ExecutionException) {
            Throwable cause = t.getCause();
            return cause != null ? cause : t;
        }
        return t;
    }
}
