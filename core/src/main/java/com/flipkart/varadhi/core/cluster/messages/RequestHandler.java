package com.flipkart.varadhi.core.cluster.messages;

import java.util.concurrent.CompletableFuture;

public interface RequestHandler <E extends ClusterMessage> {
    CompletableFuture<ResponseMessage> handle(E message);
}
