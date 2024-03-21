package com.flipkart.varadhi.config;

import com.flipkart.varadhi.components.ComponentKind;
import lombok.Data;

@Data
public class MemberConfig {
    private String nodeId;
    private ComponentKind[] roles;
    private int cpuCount;
    private int nicMBps;
}
