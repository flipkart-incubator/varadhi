package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.entities.cluster.NodeCapacity;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class TopicCapacityPolicy implements Comparable<TopicCapacityPolicy> {
    private int qps;
    private int throughputKBps;
    private int readFanOut;

    public TopicCapacityPolicy(int qps, int throughputKBps, int readFanOut) {
        this.qps = qps;
        this.throughputKBps = throughputKBps;
        this.readFanOut = readFanOut;
    }
    public TopicCapacityPolicy from(double factor, int readFanOut) {
        int qps = (int)Math.ceil((double) this.qps * factor);
        int kbps = (int)Math.ceil((double) throughputKBps * factor);
        return new TopicCapacityPolicy(qps, kbps, readFanOut);
    }

    @Override
    public int compareTo(TopicCapacityPolicy o) {
        return throughputKBps - o.throughputKBps;
    }

    @Override
    public String toString() {
        return String.format("%dKBps, %d Qps, %d readFanOut", throughputKBps, qps, readFanOut);
    }
}
