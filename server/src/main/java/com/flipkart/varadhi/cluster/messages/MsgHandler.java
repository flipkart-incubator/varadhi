package com.flipkart.varadhi.cluster.messages;

public interface MsgHandler {
    void handle(ClusterMessage message);
}
