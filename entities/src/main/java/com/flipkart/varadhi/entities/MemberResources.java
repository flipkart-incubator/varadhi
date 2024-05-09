package com.flipkart.varadhi.entities;

import lombok.Data;

@Data
public class MemberResources {
    private int cpuCount;
    private int nicMBps;

    public MemberResources(int cpuCount, int nicMBps) {
        this.cpuCount = cpuCount;
        this.nicMBps = nicMBps;
    }

}
