package com.flipkart.varadhi.pulsar.services;

import com.flipkart.varadhi.pulsar.clients.AdminClient;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.services.StorageTopicService;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;

@Slf4j
public class PulsarTopicService extends StorageTopicService<PulsarStorageTopic> {

    AdminClient adminClient;

    public PulsarTopicService(PulsarAdmin pulsarAdmin) {
        adminClient = new AdminClient(pulsarAdmin);
    }

    public void create(PulsarStorageTopic topic) {
        log.debug("Call Pulsar to create the required topic.");
        adminClient.create(topic);
    }

}
