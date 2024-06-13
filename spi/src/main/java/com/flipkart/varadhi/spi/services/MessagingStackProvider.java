package com.flipkart.varadhi.spi.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.entities.StorageSubscription;
import com.flipkart.varadhi.entities.StorageTopic;

public interface MessagingStackProvider<T extends StorageTopic, O extends Offset, S extends StorageSubscription> {
    //TODO::This is likely a candidate for flattening, instead of these many factories.
    String getName();

    void init(MessagingStackOptions messagingStackOptions, ObjectMapper mapper);

    StorageTopicFactory<T> getStorageTopicFactory();

    StorageSubscriptionFactory<S, T> getSubscriptionFactory();

    StorageTopicService<T> getStorageTopicService();
    StorageSubscriptionService<S> getStorageSubscriptionService();

    ProducerFactory<T> getProducerFactory();

    ConsumerFactory<T, O> getConsumerFactory();
}
