package com.flipkart.varadhi.entities;

/**
 * Per-topic rate limiter rollout mode: off, observe-only, or enforce limits.
 */
public enum RateLimiterMode {
    disabled, shadow, enforced
}
