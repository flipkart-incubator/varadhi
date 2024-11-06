package com.flipkart.varadhi.qos;

public interface RateLimiter<R, L> {
    // assumes 1 QPS
    // for batch request (to be considered as multi QPS), consider creating a new API
    R addTrafficData(L load);
}
