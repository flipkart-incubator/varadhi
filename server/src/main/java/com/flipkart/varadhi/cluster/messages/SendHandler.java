package com.flipkart.varadhi.cluster.messages;

@FunctionalInterface
public interface SendHandler<E extends ClusterMessage> {
    void handle(E message);
}
