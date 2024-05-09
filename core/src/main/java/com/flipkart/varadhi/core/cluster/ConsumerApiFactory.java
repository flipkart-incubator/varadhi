package com.flipkart.varadhi.core.cluster;

public interface ConsumerApiFactory {
    ConsumerApi getConsumerProxy(String consumerId);
}
