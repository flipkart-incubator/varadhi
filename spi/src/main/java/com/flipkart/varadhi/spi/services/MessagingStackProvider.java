package com.flipkart.varadhi.spi.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.varadhi.entities.StorageSubscription;
import com.flipkart.varadhi.entities.StorageTopic;

public interface MessagingStackProvider {

    String getName();

    void init(MessagingStackOptions messagingStackOptions, ObjectMapper mapper);

    StorageTopicFactory<? extends StorageTopic> getStorageTopicFactory();

    StorageSubscriptionFactory<? extends StorageSubscription<? extends StorageTopic>> getSubscriptionFactory();

    StorageTopicService getStorageTopicService();

    StorageSubscriptionService getStorageSubscriptionService();

    ProducerFactory getProducerFactory();

    ConsumerFactory getConsumerFactory();
}
