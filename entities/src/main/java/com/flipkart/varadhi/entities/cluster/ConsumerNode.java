package com.flipkart.varadhi.entities.cluster;

import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import lombok.Getter;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import static java.util.Comparator.comparing;

@Getter
public class ConsumerNode {
    // Consumer Node info as viewed by Controller
    public static Comparator<ConsumerNode> NodeComparator = comparing(o -> o.available);
    private final String consumerId;
    private final NodeCapacity available;
    private boolean markedForDeletion;
    private final Map<String, Assignment> assignments;

    public ConsumerNode(MemberInfo memberInfo) {
        this.consumerId = memberInfo.hostname();
        this.markedForDeletion = false;
        this.available = memberInfo.provisionedCapacity().clone();
        this.assignments = new HashMap<>();
    }

    public void markForDeletion() {
        this.markedForDeletion = true;
    }

    public void updateWithConsumerInfo(ConsumerInfo consumerInfo) {
        available.setMaxThroughputKBps(consumerInfo.getAvailable().getMaxThroughputKBps());
    }

    public synchronized void allocate(Assignment a, TopicCapacityPolicy requests) {
        if (null == assignments.putIfAbsent(a.getName(), a)) {
            available.setMaxThroughputKBps(available.getMaxThroughputKBps() - requests.getThroughputKBps());
        }
    }
    public synchronized void free(Assignment a, TopicCapacityPolicy requests) {
        if (null != assignments.remove(a.getName())){
            available.setMaxThroughputKBps(available.getMaxThroughputKBps() + requests.getThroughputKBps());
        }
    }
}
