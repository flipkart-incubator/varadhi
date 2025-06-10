package com.flipkart.varadhi.core.cluster;

import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.core.cluster.messages.ResponseMessage;

import java.util.concurrent.CompletableFuture;

public interface RequestHandler {
    CompletableFuture<ResponseMessage> handle(ClusterMessage message);
}
