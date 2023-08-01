package com.flipkart.varadhi.spi.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.varadhi.entities.StorageTopic;

public interface MessagingStackProvider {
    //TODO::This is likely a candidate for flattening, instead of these many factories.

    void init(MessagingStackOptions messagingStackOptions, ObjectMapper mapper);

    <T extends StorageTopic> StorageTopicFactory<T> getStorageTopicFactory();

    <T extends StorageTopic> StorageTopicService<T> getStorageTopicService();

    <T extends StorageTopic> ProducerFactory<T> getProducerFactory();
}
