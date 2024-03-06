package com.flipkart.varadhi.spi.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.entities.StorageTopic;

public interface MessagingStackProvider<T extends StorageTopic, O extends Offset> {
    //TODO::This is likely a candidate for flattening, instead of these many factories.

    void init(MessagingStackOptions messagingStackOptions, ObjectMapper mapper);

    StorageTopicFactory<T> getStorageTopicFactory();

    StorageTopicService<T> getStorageTopicService();

    ProducerFactory<T> getProducerFactory();

    ConsumerFactory<T, O> getConsumerFactory();
}
