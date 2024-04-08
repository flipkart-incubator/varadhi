package com.flipkart.varadhi.config;

import com.flipkart.varadhi.core.cluster.ComponentKind;
import lombok.Data;

@Data
public class MemberConfig {
    private String memberId;
    private ComponentKind[] roles;
    private int cpuCount;
    private int nicMBps;
}
