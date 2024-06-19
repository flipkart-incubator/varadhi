package com.flipkart.varadhi.entities.cluster;

import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
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

    public synchronized void allocate(TopicCapacityPolicy requests) {
        maxQps -= requests.getQps();
        maxThroughputKBps -= requests.getThroughputKBps();
    }

    public synchronized void free(TopicCapacityPolicy requests) {
        maxQps += requests.getQps();
        maxThroughputKBps += requests.getThroughputKBps();
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
