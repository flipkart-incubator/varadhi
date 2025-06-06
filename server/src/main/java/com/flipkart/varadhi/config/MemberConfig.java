package com.flipkart.varadhi.config;

import com.flipkart.varadhi.core.cluster.entities.ComponentKind;
import lombok.Data;

@Data
public class MemberConfig {
    private ComponentKind[] roles;
    private int clusterPort;
    private int maxQps;
    private int networkMBps;
    private int cpuCount;
}
