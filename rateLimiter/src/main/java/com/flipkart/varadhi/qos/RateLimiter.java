package com.flipkart.varadhi.qos;

import com.flipkart.varadhi.qos.entity.RateLimiterType;

public interface RateLimiter {
    RateLimiterType getType();

    // assumes 1 QPS
    // for batch request (to be considered as multi QPS), consider creating a new API
    boolean isAllowed(long value);
}
