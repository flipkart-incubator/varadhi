package com.flipkart.varadhi.config;

import com.flipkart.varadhi.entities.cluster.ComponentKind;
import lombok.Data;

@Data
public class MemberConfig {
    private ComponentKind[] roles;
    private int cpuCount;
    private int networkMBps;
}
