package com.flipkart.varadhi.controller.entities;

import com.flipkart.varadhi.core.cluster.MemberInfo;
import com.flipkart.varadhi.entities.MemberResources;
import lombok.Getter;

@Getter
public class ConsumerNode {
    private final MemberInfo memberInfo;
    private boolean markedForDeletion;
    private final MemberResources available;
    public ConsumerNode(MemberInfo memberInfo) {
        this.memberInfo = memberInfo;
        this.markedForDeletion = false;
        this.available = new MemberResources(memberInfo.capacity().getCpuCount(), memberInfo.capacity().getNicMBps());
    }
    public void markForDeletion() {
        this.markedForDeletion = true;
    }
    public void allocate(MemberResources requests) {
        available.setNicMBps(available.getNicMBps() - requests.getNicMBps());
    }

    public void free(MemberResources allocated) {
        available.setNicMBps(available.getNicMBps() + allocated.getNicMBps());
    }
}
