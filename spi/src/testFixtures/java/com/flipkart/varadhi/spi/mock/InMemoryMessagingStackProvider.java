package com.flipkart.varadhi.spi.mock;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.varadhi.spi.services.ConsumerFactory;
import com.flipkart.varadhi.spi.services.MessagingStackOptions;
import com.flipkart.varadhi.spi.services.MessagingStackProvider;
import com.flipkart.varadhi.spi.services.ProducerFactory;
import com.flipkart.varadhi.spi.services.StorageSubscriptionFactory;
import com.flipkart.varadhi.spi.services.StorageSubscriptionService;
import com.flipkart.varadhi.spi.services.StorageTopicFactory;
import com.flipkart.varadhi.spi.services.StorageTopicService;

public class InMemoryMessagingStackProvider implements MessagingStackProvider {

    @Override
    public String getName() {
        return "in-memory";
    }

    @Override
    public void init(MessagingStackOptions messagingStackOptions, ObjectMapper mapper) {

    }

    @Override
    public StorageTopicFactory<InMemoryStorageTopic> getStorageTopicFactory() {
        return new InMemoryStorageTopicFactory();
    }

    @Override
    public StorageSubscriptionFactory<InMemoryStorageSubscription> getSubscriptionFactory() {
        throw new UnsupportedOperationException("InMemoryStorageSubscriptionFactory is not implemented yet.");
    }

    @Override
    public StorageTopicService getStorageTopicService() {
        return new InMemoryStorageTopicService();
    }

    @Override
    public StorageSubscriptionService getStorageSubscriptionService() {
        throw new UnsupportedOperationException("InMemoryStorageSubscriptionService is not implemented yet.");
    }

    @Override
    public ProducerFactory getProducerFactory() {
        return new InMemoryProducerFactory();
    }

    @Override
    public ConsumerFactory getConsumerFactory() {
        throw new UnsupportedOperationException("InMemoryConsumerFactory is not implemented yet.");
    }
}
