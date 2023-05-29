package com.flipkart.varadhi.services;

import com.flipkart.varadhi.entities.StorageTopic;

public interface StorageTopicServiceFactory<T extends StorageTopic> {
    StorageTopicService<T> get();
}
