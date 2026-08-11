package com.flipkart.varadhi.common.utils;

import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeExecutor;
import dev.failsafe.Fallback;
import dev.failsafe.RetryPolicy;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;

/**
 * Failsafe helpers. Same shape as oncall {@code RetryUtils}: factory methods return a configured
 * {@link FailsafeExecutor}; callers invoke {@code getAsync}/{@code getStageAsync} on it.
 */
public final class RetryUtils {

    private RetryUtils() {
    }

    /**
     * Polling executor: retries while {@code retryOn} is thrown; any other exception fails
     * immediately. Exhausted retries complete with {@link TimeoutException}. Pair with
     * {@link FailsafeExecutor#getAsync}.
     */
    public static <T> FailsafeExecutor<T> newPollingExecutor(
        Executor executor,
        int maxAttempts,
        long delayInMs,
        Class<? extends Throwable> retryOn
    ) {
        RetryPolicy<T> policy = RetryPolicy.<T>builder()
                                           .withMaxAttempts(maxAttempts)
                                           .withDelay(Duration.ofMillis(delayInMs))
                                           .handle(retryOn)
                                           .build();
        Fallback<T> exhaustion = Fallback.<T>builderOfException(
            e -> new TimeoutException("timeout: result polling exhausted after " + maxAttempts + " attempts")
        ).handle(retryOn).build();
        return Failsafe.with(exhaustion).compose(policy).with(executor);
    }
}
