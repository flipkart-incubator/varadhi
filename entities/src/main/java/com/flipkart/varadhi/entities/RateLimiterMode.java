package com.flipkart.varadhi.entities;

/**
 * Per-topic rate limiter rollout mode (VIP-0001 §10.2).
 */
public enum RateLimiterMode {
    disabled, shadow, enforced
}
