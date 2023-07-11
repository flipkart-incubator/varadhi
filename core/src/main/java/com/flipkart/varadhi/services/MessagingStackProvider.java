package com.flipkart.varadhi.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.StorageTopicFactory;

public interface MessagingStackProvider {

    void init(MessagingStackOptions messagingStackOptions, ObjectMapper mapper);

    <T extends StorageTopic> StorageTopicFactory<T> getStorageTopicFactory();

    <T extends StorageTopic> StorageTopicService<T> getStorageTopicService();
}
