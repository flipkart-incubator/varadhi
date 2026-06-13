package com.flipkart.varadhi.core.cluster;

import java.util.concurrent.CompletableFuture;

public interface MembershipListener {
    /**
     * @param nodeId     cluster-assigned id (same id as {@link #left(String)})
     * @param memberInfo member metadata advertised at join time
     */
    CompletableFuture<Void> joined(String nodeId, MemberInfo memberInfo);

    CompletableFuture<Void> left(String id);
}
