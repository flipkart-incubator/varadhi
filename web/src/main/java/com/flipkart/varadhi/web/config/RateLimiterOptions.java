package com.flipkart.varadhi.web.config;

import com.flipkart.varadhi.entities.RateLimiterMode;
import lombok.Data;

@Data
public class RateLimiterOptions {

    private boolean enabled = false;
    private RateLimiterMode defaultMode = RateLimiterMode.disabled;
    private double fallbackBuffer = 0.25;
    private double burstSeconds = 1.0;
    private double minPodShare = 1.0;
    private long idleBucketTtlSeconds = 3600;
    private int defaultMsgSizeBytes = 1024;
}
