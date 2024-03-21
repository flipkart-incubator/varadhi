package com.flipkart.varadhi.cluster;

public interface MembershipListener {
    void joined(MemberInfo memberInfo);

    void left(String id);
}
