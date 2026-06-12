package com.flipkart.varadhi.entities;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class TopicCapacityPolicy implements Comparable<TopicCapacityPolicy> {
    private int qps;
    private int throughputKBps;
    private int readFanOut;
    private int retentionPeriodInDays;

    public TopicCapacityPolicy(int qps, int throughputKBps, int readFanOut, int retentionPeriodInDays) {
        this.qps = qps;
        this.throughputKBps = throughputKBps;
        this.readFanOut = readFanOut;
        this.retentionPeriodInDays = retentionPeriodInDays;
    }

    public TopicCapacityPolicy from(double factor, int readFanOut, int retentionPeriodInDays) {
        int qps = (int)Math.ceil((double)this.qps * factor);
        int kbps = (int)Math.ceil((double)throughputKBps * factor);
        return new TopicCapacityPolicy(qps, kbps, readFanOut, retentionPeriodInDays);
    }

    @Override
    public int compareTo(TopicCapacityPolicy o) {
        return throughputKBps - o.throughputKBps;
    }

    @Override
    public String toString() {
        return String.format("%dKBps, %d Qps, %d readFanOut", throughputKBps, qps, readFanOut);
    }

    public static TopicCapacityPolicy getDefault() {
        return new TopicCapacityPolicy(100, 1000, 1, 2);
    }

    /**
     * Floors {@link #qps} and {@link #throughputKBps} at 1 (VIP-0001 §8/§11.1).
     */
    public static int floorRate(int value) {
        return Math.max(1, value);
    }

    /**
     * Returns a copy of this policy with {@link #qps} and {@link #throughputKBps} floored at 1.
     */
    public TopicCapacityPolicy applyFloors() {
        return new TopicCapacityPolicy(floorRate(qps), floorRate(throughputKBps), readFanOut, retentionPeriodInDays);
    }
}
