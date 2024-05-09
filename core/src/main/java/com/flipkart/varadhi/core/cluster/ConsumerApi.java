package com.flipkart.varadhi.core.cluster;

public interface ConsumerApi {
    void start(ShardOperation.StartData operation);
}
