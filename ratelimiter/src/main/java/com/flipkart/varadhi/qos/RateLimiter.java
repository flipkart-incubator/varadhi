package com.flipkart.varadhi.qos;

public interface RateLimiter<Param, Result> {
    Result addTrafficData(Param load);
}
