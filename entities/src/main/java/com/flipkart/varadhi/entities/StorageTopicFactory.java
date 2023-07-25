package com.flipkart.varadhi.entities;

public interface StorageTopicFactory<T extends StorageTopic> {
    T getTopic(Project project, String topicName, CapacityPolicy capacityPolicy);
}
