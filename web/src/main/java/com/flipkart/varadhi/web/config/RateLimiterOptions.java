package com.flipkart.varadhi.web.config;

import com.flipkart.varadhi.entities.RateLimiterMode;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
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
    @NotNull
    private RateLimiterMode defaultMode = RateLimiterMode.disabled;

    /**
     * Fractional headroom applied to the per-pod even split
     * ({@code regionBudget / podCount × (1 + fallbackBuffer)}). In {@code [0, 1)}.
     */
    @DecimalMin ("0.0")
    @DecimalMax (value = "1.0", inclusive = false)
    private double fallbackBuffer = 0.25;

    /**
     * Token-bucket window in seconds; bucket capacity = {@code rate × windowSecs}. Must be {@code >= 1}
     * ({@code 0} yields a zero-capacity bucket that rejects all traffic).
     */
    @Min (1)
    private int windowSecs = 1;

    /**
     * Minimum per-pod QPS budget floor so low-throughput topics are not starved when split across many pods.
     * Worst-case aggregate over-allow is bounded at {@code podCount × minPodQps}.
     * 
     * TODO: candidate for removal. As there is no real use for configurability. decide later.
     */
    @Min (1)
    private int minPodQps = 1;
}
