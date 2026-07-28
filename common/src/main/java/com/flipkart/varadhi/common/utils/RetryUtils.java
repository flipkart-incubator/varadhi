package com.flipkart.varadhi.common.utils;

import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeExecutor;
import dev.failsafe.Fallback;
import dev.failsafe.RetryPolicy;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

/**
 * Failsafe helpers. Same shape as oncall {@code RetryUtils}: factory methods return a configured
 * {@link FailsafeExecutor}; callers invoke {@code getAsync}/{@code getStageAsync} on it.
 */
public final class RetryUtils {

    private RetryUtils() {
    }

    /**
     * Result-polling executor: retries while {@code retryOnResult} matches; probe exceptions abort
     * immediately via {@code abortOn(Exception)} and propagate (not rewritten as timeout). Exhausted
     * result retries complete exceptionally with {@link TimeoutException}. Pair with
     * {@link FailsafeExecutor#getAsync}.
     */
    public static <T> FailsafeExecutor<T> newPollingExecutor(
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
        // RetryPolicy alone returns the last matching result when attempts are exhausted; wrap with
        // a Fallback so callers always see exhaustion as a failure. handleIf (not handleResultIf)
        // replaces Failsafe's default "handle all exceptions" — otherwise abortOn probe failures
        // would be rewritten as TimeoutException here.
        Fallback<T> exhaustion = Fallback.<T>builderOfException(
            e -> new TimeoutException("timeout: result polling exhausted after " + maxAttempts + " attempts")
        ).handleIf((result, failure) -> failure == null && retryOnResult.test(result)).build();
        return Failsafe.with(exhaustion).compose(policy).with(executor);
    }
}
