package com.flipkart.varadhi.entities.cluster;

import lombok.Data;

@Data
public class MemberResources {
    private int cpuCount;
    private int networkMBps;

    public MemberResources(int cpuCount, int networkMBps) {
        this.cpuCount = cpuCount;
        this.networkMBps = networkMBps;
    }

}
