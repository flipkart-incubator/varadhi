package com.flipkart.varadhi.common.utils;

import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeExecutor;
import dev.failsafe.RetryPolicy;
import dev.failsafe.RetryPolicyBuilder;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.Executor;
import java.util.function.Predicate;

/**
 * For handling retries, specifically of {@link java.util.concurrent.CompletableFuture}s.
 *
 * <p>API mirrors the oncall {@code com.flipkart.varadhi.utils.RetryUtils} naming schema; this
 * implementation uses {@code dev.failsafe} (Failsafe 3.x).
 */
@Slf4j
public final class RetryUtils {

    private static final double DEFAULT_JITTER_FACTOR = 0.2;
    private static final long MAX_RETRY_DELAY_MS = 90_000L;
    private static final double BACKOFF_FACTOR = 1.2;

    private RetryUtils() {
    }

    private static <T> RetryPolicyBuilder<T> retryPolicyWithJitter() {
        return RetryPolicy.<T>builder()
                          .withMaxAttempts(Integer.MAX_VALUE)
                          .withDelay(Duration.ofSeconds(1))
                          .withJitter(DEFAULT_JITTER_FACTOR)
                          .onFailedAttempt(RetryUtils::logFailedAttempt)
                          .onRetriesExceeded(RetryUtils::logRetriesExceeded);
    }

    private static <T> RetryPolicyBuilder<T> retryPolicyWithBackoff(int maxAttempts, int delayInSec) {
        return retryPolicyWithBackoffInMs(maxAttempts, delayInSec * 1000L);
    }

    private static <T> RetryPolicyBuilder<T> retryPolicyWithBackoff(int maxAttempts) {
        return retryPolicyWithBackoff(maxAttempts, 1);
    }

    private static <T> RetryPolicyBuilder<T> retryPolicyWithBackoffInMs(int maxAttempts, long delayInMs) {
        return retryPolicyWithBackoffInMs(maxAttempts, delayInMs, MAX_RETRY_DELAY_MS);
    }

    private static <T> RetryPolicyBuilder<T> retryPolicyWithBackoffInMs(
        int maxAttempts,
        long delayInMs,
        long maxDelayInMs
    ) {
        return RetryPolicy.<T>builder()
                          .withMaxAttempts(maxAttempts)
                          .withBackoff(delayInMs, maxDelayInMs, ChronoUnit.MILLIS, BACKOFF_FACTOR)
                          .onFailedAttempt(RetryUtils::logFailedAttempt)
                          .onRetriesExceeded(RetryUtils::logRetriesExceeded);
    }

    private static void logFailedAttempt(dev.failsafe.event.ExecutionAttemptedEvent<?> e) {
        log.error("Method execution failed, attempt no. {}", e.getAttemptCount(), e.getLastException());
    }

    private static void logRetriesExceeded(dev.failsafe.event.ExecutionCompletedEvent<?> e) {
        log.error("Future failed after {} attempts.", e.getAttemptCount());
    }

    public static <T> Builder<T> newPolicy() {
        return new Builder<>(retryPolicyWithJitter());
    }

    public static <T> Builder<T> newBackoffPolicy() {
        return new Builder<>(retryPolicyWithBackoff(Integer.MAX_VALUE));
    }

    public static <T> Builder<T> newBackoffPolicy(int maxAttempts, int delayInSec) {
        return new Builder<>(retryPolicyWithBackoff(maxAttempts, delayInSec));
    }

    public static <T> Builder<T> newBackoffPolicyInMs(int maxAttempts, long delayInMs) {
        return newBackoffPolicyInMs(maxAttempts, delayInMs, MAX_RETRY_DELAY_MS);
    }

    public static <T> Builder<T> newBackoffPolicyInMs(int maxAttempts, long delayInMs, long maxDelayInMs) {
        return new Builder<>(retryPolicyWithBackoffInMs(maxAttempts, delayInMs, maxDelayInMs));
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

    /**
     * Runs {@code probe} on {@code executor}, retrying while {@code retryOnResult} matches, up to
     * {@code maxAttempts}.
     */
    public static <T> java.util.concurrent.CompletableFuture<T> getAsync(
        Executor executor,
        int maxAttempts,
        long delayInMs,
        Predicate<T> retryOnResult,
        java.util.function.Supplier<T> probe
    ) {
        RetryPolicyBuilder<T> policy = RetryPolicy.<T>builder()
                                                  .withMaxAttempts(maxAttempts)
                                                  .withDelay(Duration.ofMillis(delayInMs))
                                                  .onFailedAttempt(RetryUtils::logFailedAttempt)
                                                  .onRetriesExceeded(RetryUtils::logRetriesExceeded);
        return new Builder<>(policy).retryOnResultIf(retryOnResult).withExecutor(executor).getAsync(probe::get);
    }

    /**
     * A builder over {@link RetryPolicy} with high-level configuration methods.
     */
    public static final class Builder<T> {
        private final RetryPolicyBuilder<T> policyBuilder;

        private Builder(RetryPolicyBuilder<T> policyBuilder) {
            this.policyBuilder = policyBuilder;
        }

        @SuppressWarnings ("unchecked")
        public Builder<T> retryIf(Predicate<? extends Throwable> failurePredicate) {
            Predicate<Throwable> predicate = (Predicate<Throwable>)failurePredicate;
            policyBuilder.handleIf(predicate::test);
            return this;
        }

        public Builder<T> retryOnResultIf(Predicate<T> resultPredicate) {
            policyBuilder.handleResultIf(r -> resultPredicate.test(r));
            return this;
        }

        public RetryPolicy<T> build() {
            return policyBuilder.build();
        }

        /**
         * Runs the retriable task and all retries on the given executor.
         */
        public FailsafeExecutor<T> withExecutor(Executor executor) {
            return Failsafe.with(policyBuilder.build()).with(executor);
        }

        /**
         * Runs the retriable task on the calling thread (direct executor).
         */
        public FailsafeExecutor<T> toExecutor() {
            return Failsafe.with(policyBuilder.build());
        }
    }
}
