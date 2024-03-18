package com.flipkart.varadhi.core.cluster.messages;

@FunctionalInterface
public interface SendHandler<E extends ClusterMessage> {
    void handle(E message);
}
