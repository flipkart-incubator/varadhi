package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.entities.CapacityPolicy;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.StorageTopic;

public interface StorageTopicFactory<T extends StorageTopic> {
    T getTopic(Project project, String topicName, CapacityPolicy capacityPolicy);
}
