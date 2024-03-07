package com.flipkart.varadhi.core.cluster.messages;

import java.util.concurrent.CompletableFuture;

@FunctionalInterface
public interface SendHandler<E extends ClusterMessage> {
    CompletableFuture<Void> handle(E message);
}
