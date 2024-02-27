package com.flipkart.varadhi.cluster;

import java.util.concurrent.CompletableFuture;

/**
 * This is subset of methods from vertx.EventBus which are node specific. Instead of using address concept, we will
 * rely on path concept from http. address is not needed as this class is meant to represent a message channel to a specific
 * node.
 * publish methods are fire & forget.
 * send methods don't have any response, but their delivery can be tracked using the future.
 * request methods are for traditional request-response pattern.
 *
 * TODO: whether to use the ClusterMessage class. It is unlikely we will use reply-to-reply feature.
 */
public interface MessageChannel {

    void publish(String path, ClusterMessage msg);

    CompletableFuture<Void> send(String path, ClusterMessage msg);

    CompletableFuture<ResponseMessage> request(String path, ClusterMessage msg);

    void addMessageHandler(String path, MessageHandler messageHandler);

    void removeMessageHandler(String path);
}
