package com.flipkart.varadhi.cluster;

public interface MembershipListener {
    void joined(NodeInfo nodeInfo);

    void left(NodeInfo nodeInfo);
}
