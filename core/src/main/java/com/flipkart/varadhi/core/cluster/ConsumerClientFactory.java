package com.flipkart.varadhi.core.cluster;

public interface ConsumerClientFactory {
    ConsumerApi getInstance(String consumerId);
}
