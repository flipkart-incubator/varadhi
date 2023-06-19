package com.flipkart.varadhi.services;

import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.StorageTopicFactory;

public interface MessagingStackProvider {

    void init(MessagingStackOptions messagingStackOptions);

    <T extends StorageTopic> StorageTopicFactory<T> getStorageTopicFactory();

    <T extends StorageTopic> StorageTopicService<T> getStorageTopicService();
}
