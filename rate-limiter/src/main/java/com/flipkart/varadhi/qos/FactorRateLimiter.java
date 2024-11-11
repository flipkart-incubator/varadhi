package com.flipkart.varadhi.qos;

public interface FactorRateLimiter extends RateLimiter<Long, Boolean> {
    void updateSuppressionFactor(double suppressionFactor);
}
