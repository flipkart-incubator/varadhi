package com.flipkart.varadhi.pulsar.services;

import com.flipkart.varadhi.pulsar.clients.AdminClient;
import com.flipkart.varadhi.pulsar.config.PulsarClientOptions;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.services.StorageTopicService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PulsarTopicService extends StorageTopicService<PulsarStorageTopic> {

    AdminClient adminClient;

    public PulsarTopicService(PulsarClientOptions pulsarClientOptions) {
        adminClient = new AdminClient(pulsarClientOptions);
    }

    public void create(PulsarStorageTopic topic) {
        log.debug("Call Pulsar to create the required topic.");
        adminClient.create(topic);
    }

}
