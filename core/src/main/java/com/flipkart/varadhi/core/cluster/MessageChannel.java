package com.flipkart.varadhi.core.cluster;


import com.flipkart.varadhi.core.cluster.messages.*;

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

    void publish(String apiPath, ClusterMessage msg);

    CompletableFuture<Void> send(String apiPath, ClusterMessage msg);

    CompletableFuture<ResponseMessage> request(String apiPath, ClusterMessage msg);

    <E extends ClusterMessage> void register(String basePath, Class<E> messageClazz, SendHandler<E> handler);

    void removeMessageHandler(String apiPath);

    enum Method {
        SEND("send"),
        PUBLISH("publish"),
        REQUEST("request");

        private final String name;

        Method(String name) {
            this.name = name;
        }
        @Override
        public String toString() {
            return name;
        }
    }
}
