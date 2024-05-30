package com.flipkart.varadhi.entities.cluster;

import com.flipkart.varadhi.entities.CapacityPolicy;
import lombok.Getter;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import static java.util.Comparator.comparing;

@Getter
public class ConsumerNode {
    public static Comparator<ConsumerNode> NodeComparator = comparing(o -> o.available);
    private final MemberInfo memberInfo;
    private final CapacityPolicy available;
    private boolean markedForDeletion;
    private final Map<String, Assignment> assignments;

    public ConsumerNode(MemberInfo memberInfo) {
        this.memberInfo = memberInfo;
        this.markedForDeletion = false;
        this.available = new CapacityPolicy(1000, memberInfo.capacity().getNetworkMBps() * 1000);
        this.assignments = new HashMap<>();
    }

    public void markForDeletion() {
        this.markedForDeletion = true;
    }

    public void updateWithConsumerInfo(ConsumerInfo consumerInfo) {
        available.setMaxThroughputKBps(consumerInfo.getAvailable().getMaxThroughputKBps());
    }

    public String getConsumerId() {
        return memberInfo.hostname();
    }

    public synchronized void allocate(Assignment a, CapacityPolicy requests) {
        if (null == assignments.putIfAbsent(a.getName(), a)) {
            available.setMaxThroughputKBps(available.getMaxThroughputKBps() - requests.getMaxThroughputKBps());
        }
    }
    public synchronized void free(Assignment a, CapacityPolicy requests) {
        if (null != assignments.remove(a.getName())){
            available.setMaxThroughputKBps(available.getMaxThroughputKBps() + requests.getMaxThroughputKBps());
        }
    }
}
