package com.flipkart.varadhi.core.cluster.api;

public interface ConsumerClientFactory {
    ConsumerApi getInstance(String consumerId);
}
