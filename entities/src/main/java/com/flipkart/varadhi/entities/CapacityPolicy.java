package com.flipkart.varadhi.entities;

import lombok.Data;

@Data
public class CapacityPolicy {
    private int maxThroughputKBps;
    private int maxQPS;

    public CapacityPolicy(int maxQPS, int maxThroughputKBps) {
        this.maxQPS = maxQPS;
        this.maxThroughputKBps = maxThroughputKBps;
    }

    public static CapacityPolicy getDefault() {
        return new CapacityPolicy(100, 100);
    }
}
