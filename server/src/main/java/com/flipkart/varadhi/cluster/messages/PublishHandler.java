package com.flipkart.varadhi.cluster.messages;

public interface PublishHandler<E extends ClusterMessage> {
    void handle(E message);
}
