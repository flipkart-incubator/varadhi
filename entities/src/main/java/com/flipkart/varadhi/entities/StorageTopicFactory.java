package com.flipkart.varadhi.entities;

public interface StorageTopicFactory<T extends StorageTopic> {
    T get(String name, CapacityPolicy capacityPolicy);
}
