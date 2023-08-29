package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.entities.CapacityPolicy;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.StorageTopic;

public interface StorageTopicFactory<T extends StorageTopic> {

    // TODO:: This will change further to take care of producing and replicating regions as well.

    // topicName is globally unique. Messaging stack can take a dependency on this to create either
    // global or regional topic names as required.
    T getTopic(String topicName, Project project, CapacityPolicy capacityPolicy);
}
