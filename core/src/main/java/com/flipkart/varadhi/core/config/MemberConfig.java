package com.flipkart.varadhi.core.config;

import com.flipkart.varadhi.core.cluster.ComponentKind;
import jakarta.validation.constraints.NotEmpty;
import lombok.Data;

@Data
public class MemberConfig {
    @NotEmpty
    private ComponentKind[] roles;
    private int clusterPort;
    private int maxQps;
    private int networkMBps;
    private int cpuCount;
}
