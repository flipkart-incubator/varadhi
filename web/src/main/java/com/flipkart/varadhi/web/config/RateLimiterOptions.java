package com.flipkart.varadhi.web.config;

import com.flipkart.varadhi.entities.RateLimiterMode;
import lombok.Data;

/**
 * Deployment-level configuration for the produce rate limiter.
 * <p>
 * Wired via {@link WebConfiguration} and the {@code rateLimiterOptions} block in
 * {@code conf/configuration.yml}.
 */
@Data
public class RateLimiterOptions {

    /**
     * Deployment master kill switch. When {@code false}, the produce rate limiter is a no-op
     * pass-through (no buckets, no 429) regardless of per-topic {@code rateLimiterMode}.
     */
    private boolean enabled = false;

    /** Rate-limiter mode for topics that do not set an explicit {@code rateLimiterMode}. */
    private RateLimiterMode defaultMode = RateLimiterMode.disabled;

    /**
     * Fractional headroom applied to the per-pod even split
     * ({@code regionBudget / podCount × (1 + fallbackBuffer)}).
     */
    private double fallbackBuffer = 0.25;

    /**
     * Token-bucket window in seconds; bucket capacity = {@code rate × windowSecs}.
     */
    private int windowSecs = 1;

    /**
     * Minimum per-pod QPS budget floor so low-throughput topics are not starved when split across many pods.
     * Worst-case aggregate over-allow is bounded at {@code podCount × minPodQps}.
     * 
     * TODO: candidate for removal. As there is no real use for configurability. decide later.
     */
    private int minPodQps = 1;
}
