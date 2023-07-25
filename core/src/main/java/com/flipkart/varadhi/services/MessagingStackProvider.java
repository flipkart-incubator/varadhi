package com.flipkart.varadhi.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.varadhi.entities.ProducerFactory;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.StorageTopicFactory;

public interface MessagingStackProvider {
    //TODO::This is likely a candidate for flattening, instead of these many factories.

    void init(MessagingStackOptions messagingStackOptions, ObjectMapper mapper);

    <T extends StorageTopic> StorageTopicFactory<T> getStorageTopicFactory();

    <T extends StorageTopic> StorageTopicService<T> getStorageTopicService();

    <T extends StorageTopic> ProducerFactory<T> getProducerFactory();
}
