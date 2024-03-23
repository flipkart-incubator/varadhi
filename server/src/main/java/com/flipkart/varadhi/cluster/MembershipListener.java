package com.flipkart.varadhi.cluster;

import com.flipkart.varadhi.core.cluster.MemberInfo;

public interface MembershipListener {
    void joined(MemberInfo memberInfo);

    void left(String id);
}
