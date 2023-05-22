package com.flipkart.varadhi.pulsar;

import com.flipkart.varadhi.services.StorageTopicService;
import com.flipkart.varadhi.services.StorageTopicServiceFactory;

public class PulsarTopicServiceFactory implements StorageTopicServiceFactory<PulsarStorageTopic> {
    StorageTopicService<PulsarStorageTopic> pulsarTopicService;

    public PulsarTopicServiceFactory() {
        pulsarTopicService = new PulsarTopicService();
    }
    @Override
    public StorageTopicService<PulsarStorageTopic> get() {
        return pulsarTopicService;
    }
}
