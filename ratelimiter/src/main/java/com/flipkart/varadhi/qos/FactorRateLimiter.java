package com.flipkart.varadhi.qos;

public interface FactorRateLimiter extends RateLimiter<Boolean, Long> {
    void updateSuppressionFactor(double suppressionFactor);
}
