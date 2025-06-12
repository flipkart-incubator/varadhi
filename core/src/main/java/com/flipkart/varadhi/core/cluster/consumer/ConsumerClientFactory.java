package com.flipkart.varadhi.core.cluster.consumer;

public interface ConsumerClientFactory {
    ConsumerApi getInstance(String consumerId);
}
