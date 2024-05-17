package com.flipkart.varadhi.entities.cluster;

import lombok.Data;

@Data
public class MemberResources {
    private int cpuCount;
    private float networkMBps;

    public MemberResources(int cpuCount, float networkMBps) {
        this.cpuCount = cpuCount;
        this.networkMBps = networkMBps;
    }

}
