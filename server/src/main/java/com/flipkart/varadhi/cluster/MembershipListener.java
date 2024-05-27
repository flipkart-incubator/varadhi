package com.flipkart.varadhi.cluster;

import com.flipkart.varadhi.entities.cluster.MemberInfo;

public interface MembershipListener {
    void joined(MemberInfo memberInfo);

    void left(String id);
}
