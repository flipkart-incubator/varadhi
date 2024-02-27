package com.flipkart.varadhi.cluster;

import java.util.concurrent.CompletableFuture;

public interface MessageHandler {

    // handle will be used for messages sent via channel.send() and channel.publish().
    <E extends ClusterMessage> CompletableFuture<Void> handle(E message);

    // request will be used for messages sent via channel.request().
    <E extends ClusterMessage> CompletableFuture<ResponseMessage> request(E message);
}
