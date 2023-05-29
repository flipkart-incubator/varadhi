package com.flipkart.varadhi.entities;

public interface StorageTopicFactory<T extends StorageTopic> {
    T getTopic(String name, CapacityPolicy capacityPolicy);
}
