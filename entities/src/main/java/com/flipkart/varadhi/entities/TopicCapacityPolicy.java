package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.entities.cluster.NodeCapacity;
import lombok.Data;

@Data
public class TopicCapacityPolicy implements Comparable<NodeCapacity> {
    private int throughputKBps;
    private int qps;
    private int readFanOut;

    public TopicCapacityPolicy(int qps, int throughputKBps, int readFanOut) {
        this.qps = qps;
        this.throughputKBps = throughputKBps;
        this.readFanOut = readFanOut;
    }

    public static TopicCapacityPolicy getDefault() {
        return new TopicCapacityPolicy(100, 400, 2);
    }

    public TopicCapacityPolicy from(double factor, int readFanOut) {
        int qps = (int)Math.ceil((double) this.qps * factor);
        int kbps = (int)Math.ceil((double) throughputKBps * factor);
        return new TopicCapacityPolicy(qps, kbps, readFanOut);
    }

    @Override
    public int compareTo(NodeCapacity o) {
        return throughputKBps - o.getMaxThroughputKBps();
    }

    @Override
    public String toString() {
        return String.format("%d KBps %d qps", throughputKBps, qps);
    }
}
