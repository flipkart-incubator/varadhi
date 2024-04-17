package com.flipkart.varadhi.cluster.messages;

import java.util.concurrent.CompletableFuture;

public interface RequestHandler <E extends ClusterMessage, R extends ResponseMessage> {
    CompletableFuture<R> handle(E message);
}
