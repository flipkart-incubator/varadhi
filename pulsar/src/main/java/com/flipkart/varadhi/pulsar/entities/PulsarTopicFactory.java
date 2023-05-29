package com.flipkart.varadhi.pulsar.entities;

import com.flipkart.varadhi.entities.CapacityPolicy;
import com.flipkart.varadhi.entities.StorageTopicFactory;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;

public class PulsarTopicFactory implements StorageTopicFactory<PulsarStorageTopic> {
    @Override
    public PulsarStorageTopic getTopic(String name, CapacityPolicy capacityPolicy) {
        return PulsarStorageTopic.of(name, capacityPolicy);
    }
}
