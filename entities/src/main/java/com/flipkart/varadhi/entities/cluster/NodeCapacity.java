package com.flipkart.varadhi.entities.cluster;

import lombok.Data;

@Data
public class NodeCapacity implements Comparable<NodeCapacity> {
    private int maxThroughputKBps;
    private int maxQPS;

    public NodeCapacity(int maxQPS, int maxThroughputKBps) {
        this.maxQPS = maxQPS;
        this.maxThroughputKBps = maxThroughputKBps;
    }

    public static NodeCapacity getDefault() {
        return new NodeCapacity(100, 100);
    }

    public NodeCapacity from(double factor) {
        return new NodeCapacity((int)((double)maxQPS * factor), (int)((double)maxThroughputKBps * factor));
    }

    @Override
    public int compareTo(NodeCapacity o) {
        return maxThroughputKBps - o.getMaxThroughputKBps();
    }

    @Override
    public String toString() {
        return String.format("%d KBps %d qps", maxThroughputKBps, maxQPS);
    }
}
