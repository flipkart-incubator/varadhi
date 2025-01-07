package com.flipkart.varadhi.cluster;

import com.flipkart.varadhi.core.cluster.entities.MemberInfo;

import java.util.concurrent.CompletableFuture;

public interface MembershipListener {
    CompletableFuture<Void> joined(MemberInfo memberInfo);

    CompletableFuture<Void> left(String id);
}
