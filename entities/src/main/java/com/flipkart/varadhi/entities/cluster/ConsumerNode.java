package com.flipkart.varadhi.entities.cluster;

import com.flipkart.varadhi.entities.CapacityPolicy;
import lombok.Getter;

import java.util.Comparator;

import static java.util.Comparator.comparing;


@Getter
public class ConsumerNode {
    public static Comparator<ConsumerNode> NodeComparator = comparing(o -> o.available);
    private final MemberInfo memberInfo;
    private final CapacityPolicy available;
    private boolean markedForDeletion;

    public ConsumerNode(MemberInfo memberInfo) {
        this.memberInfo = memberInfo;
        this.markedForDeletion = false;
        this.available = new CapacityPolicy(1000, memberInfo.capacity().getNetworkMBps() * 1000);
    }

    public void markForDeletion() {
        this.markedForDeletion = true;
    }

    public void allocate(CapacityPolicy requests) {
        available.setMaxThroughputKBps(available.getMaxThroughputKBps() - requests.getMaxThroughputKBps());
    }
}
