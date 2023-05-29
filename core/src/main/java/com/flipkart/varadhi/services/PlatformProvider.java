package com.flipkart.varadhi.services;

import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.StorageTopicFactory;

public interface PlatformProvider {

    void init(PlatformOptions platformOptions);
    <T extends StorageTopic> StorageTopicFactory<T> getStorageTopicFactory();
    <T extends StorageTopic> StorageTopicServiceFactory<T> getStorageTopicServiceFactory();
}
