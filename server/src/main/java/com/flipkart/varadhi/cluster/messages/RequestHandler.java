package com.flipkart.varadhi.cluster.messages;

import java.util.concurrent.CompletableFuture;

public interface RequestHandler {
    CompletableFuture<ResponseMessage> handle(ClusterMessage message);
}
