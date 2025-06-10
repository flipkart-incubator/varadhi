package com.flipkart.varadhi.core.cluster;

import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;

public interface MsgHandler {
    void handle(ClusterMessage message);
}
