package com.flipkart.varadhi.entities.cluster;

import com.flipkart.varadhi.entities.CapacityPolicy;
import lombok.Getter;

@Getter
public class ConsumerNode {
    private final MemberInfo memberInfo;
    private boolean markedForDeletion;
    private final MemberResources available;
    public ConsumerNode(MemberInfo memberInfo) {
        this.memberInfo = memberInfo;
        this.markedForDeletion = false;
        this.available = new MemberResources(memberInfo.capacity().getCpuCount(), memberInfo.capacity().getNetworkMBps());
    }
    public void markForDeletion() {
        this.markedForDeletion = true;
    }
    public void allocate(CapacityPolicy requests) {
        float remainingThroughputMBps = available.getNetworkMBps() - (float)requests.getMaxThroughputKBps()/1000;
        available.setNetworkMBps(remainingThroughputMBps);
    }
}
