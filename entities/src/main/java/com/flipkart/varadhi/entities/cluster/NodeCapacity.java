package com.flipkart.varadhi.entities.cluster;

import lombok.Data;

@Data
public class NodeCapacity implements Comparable<NodeCapacity> {
    private int maxQps;
    private int maxThroughputKBps;

    public NodeCapacity(int maxQps, int maxThroughputKBps) {
        this.maxQps = maxQps;
        this.maxThroughputKBps = maxThroughputKBps;
    }

    public NodeCapacity clone() {
        return new NodeCapacity(maxQps, maxThroughputKBps);
    }

    @Override
    public int compareTo(NodeCapacity o) {
        return maxThroughputKBps - o.getMaxThroughputKBps();
    }

    @Override
    public String toString() {
        return String.format("%d KBps %d qps", maxThroughputKBps, maxQps);
    }
}
