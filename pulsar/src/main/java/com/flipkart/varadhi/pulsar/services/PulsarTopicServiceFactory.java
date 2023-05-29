package com.flipkart.varadhi.pulsar.services;

import com.flipkart.varadhi.pulsar.config.PulsarClientOptions;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.services.StorageTopicService;
import com.flipkart.varadhi.services.StorageTopicServiceFactory;

public class PulsarTopicServiceFactory implements StorageTopicServiceFactory<PulsarStorageTopic> {
    StorageTopicService<PulsarStorageTopic> pulsarTopicService;

    public PulsarTopicServiceFactory(PulsarClientOptions pulsarClientOptions) {
        pulsarTopicService = new PulsarTopicService(pulsarClientOptions);
    }
    @Override
    public StorageTopicService<PulsarStorageTopic> get() {
        return pulsarTopicService;
    }
}
