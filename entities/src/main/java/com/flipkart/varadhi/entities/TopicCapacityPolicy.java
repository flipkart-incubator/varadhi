package com.flipkart.varadhi.entities;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;

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
     * Whether {@code throughputKBps} can sustain {@code qps} at the profile's maximum message size.
     */
    public boolean isConsistentWith(MessageSizeProfile messageSizeProfile) {
        Objects.requireNonNull(messageSizeProfile);
        long maxRequiredBytesPerSec = (long)qps * messageSizeProfile.getMaxMsgSizeBytes();
        long actualBytesPerSec = (long)throughputKBps * 1024L;
        return actualBytesPerSec >= maxRequiredBytesPerSec;
    }
}
