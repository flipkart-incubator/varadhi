package com.flipkart.varadhi.produce.ratelimit;

/**
 * Callbacks from {@link ProduceRateLimiter#check} for normal rate-limit outcomes (VIP-0001 §12).
 * <p>
 * Phase 4 wires a {@link com.flipkart.varadhi.produce.telemetry.ProducerMetrics} adapter; until
 * then {@link #NOOP} is the default. Implementations must be non-blocking and must not throw.
 * Exceptional {@code check} failures are logged in the facade and are not part of this contract.
 */
public interface RateLimitTelemetry {

    /**
     * The topic is over quota in {@code shadow} mode — produce is still allowed.
     * <p>
     * Called at most once per {@code check} when {@code tryAcquire} fails and mode is
     * {@link com.flipkart.varadhi.entities.RateLimiterMode#shadow}. Maps to
     * {@code producer.ratelimit.would_have_throttled.count}.
     */
    void wouldHaveThrottled();

    /**
     * The topic is over quota in {@code enforced} mode — produce will be rejected (429).
     * <p>
     * Called at most once per {@code check} when {@code tryAcquire} fails and mode is
     * {@link com.flipkart.varadhi.entities.RateLimiterMode#enforced}.
     */
    void enforcedThrottled();

    /** No-op implementation for disabled limiters and until metrics are wired. */
    RateLimitTelemetry NOOP = new RateLimitTelemetry() {
        @Override
        public void wouldHaveThrottled() {
        }

        @Override
        public void enforcedThrottled() {
        }
    };
}
