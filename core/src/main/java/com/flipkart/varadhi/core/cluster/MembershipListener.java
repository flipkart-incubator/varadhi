package com.flipkart.varadhi.core.cluster;

import java.util.concurrent.CompletableFuture;

public interface MembershipListener {
    CompletableFuture<Void> joined(MemberInfo memberInfo);

    CompletableFuture<Void> left(String id);
}
