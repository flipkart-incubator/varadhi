package com.flipkart.varadhi.pulsar.services;

import com.flipkart.varadhi.exceptions.NotImplementedException;
import com.flipkart.varadhi.pulsar.clients.AdminClient;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.spi.services.StorageTopicService;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;

@Slf4j
public class PulsarTopicService extends StorageTopicService<PulsarStorageTopic> {

    AdminClient adminClient;

    public PulsarTopicService(PulsarAdmin pulsarAdmin) {
        adminClient = new AdminClient(pulsarAdmin);
    }

    public void create(PulsarStorageTopic topic) {
        //TODO:: See if this can be idempotent.
        log.debug("Call Pulsar to create the required topic.");
        adminClient.create(topic);
    }

    @Override
    public PulsarStorageTopic get(String topicName) {
        throw new NotImplementedException();
    }

}
