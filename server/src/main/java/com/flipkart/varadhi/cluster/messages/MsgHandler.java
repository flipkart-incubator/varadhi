package com.flipkart.varadhi.cluster.messages;

@FunctionalInterface
public interface MsgHandler {
    void handle(ClusterMessage message);
}
