package com.flipkart.varadhi.pulsar;

import com.flipkart.varadhi.entities.CapacityPolicy;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.StorageTopicFactory;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;

public class PulsarTopicFactory implements StorageTopicFactory<PulsarStorageTopic> {
    @Override
    public PulsarStorageTopic getTopic(Project project, String topicName, CapacityPolicy capacityPolicy) {
        return PulsarStorageTopic.of(project, topicName, capacityPolicy);
    }
}
