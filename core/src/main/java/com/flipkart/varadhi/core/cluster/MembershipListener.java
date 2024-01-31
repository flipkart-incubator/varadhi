package com.flipkart.varadhi.core.cluster;

public interface MembershipListener {
    void joined(NodeInfo nodeInfo);

    void left(NodeInfo nodeInfo);
}
