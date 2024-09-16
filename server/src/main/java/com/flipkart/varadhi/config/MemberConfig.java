package com.flipkart.varadhi.config;

import com.flipkart.varadhi.entities.cluster.ComponentKind;
import lombok.Data;

@Data
public class MemberConfig {
    private ComponentKind[] roles;
    private int clusterPort;
    private int maxQps;
    private int networkMBps;
    private int cpuCount;
    private int nicMBps;
}
