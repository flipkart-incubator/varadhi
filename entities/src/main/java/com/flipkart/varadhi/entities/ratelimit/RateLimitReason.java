package com.flipkart.varadhi.entities.ratelimit;

// TODO(rl): is it needed, if so add it in impl
public enum RateLimitReason {
    NONE("Not rate limited"),
    THROUGHPUT_EXCEEDED("Throughput exceeded"),
    RATE_EXCEEDED("Rate exceeded"),
    CLUSTER_LOAD_CRITICAL("Cluster load is critical");

    final String detail;

    RateLimitReason(String detail) {
        this.detail = detail;
    }
}
