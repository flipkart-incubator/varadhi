package com.flipkart.varadhi.core.cluster.messages;

public interface PublishHandler<E extends ClusterMessage> {
    void handle(E message);
}
