package com.flipkart.varadhi.entities;

import lombok.Data;

@Data
public class CapacityPolicy implements Comparable<CapacityPolicy> {
    private int maxThroughputKBps;
    private int maxQPS; //TODO:: Evaluate if QPS is needed or can be dropped.

    public CapacityPolicy(int maxQPS, int maxThroughputKBps) {
        this.maxQPS = maxQPS;
        this.maxThroughputKBps = maxThroughputKBps;
    }

    public static CapacityPolicy getDefault() {
        return new CapacityPolicy(100, 100);
    }

    @Override
    public int compareTo(CapacityPolicy o) {
        return maxThroughputKBps - o.getMaxThroughputKBps();
    }

    @Override
    public String toString() {
        return String.format("%d KBps", maxThroughputKBps);
    }
}
