package com.flipkart.varadhi.pulsar.services;

import com.flipkart.varadhi.exceptions.MessagingException;
import com.flipkart.varadhi.exceptions.NotImplementedException;
import com.flipkart.varadhi.pulsar.clients.ClientProvider;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.spi.services.StorageTopicService;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdminException;

@Slf4j
public class PulsarTopicService extends StorageTopicService<PulsarStorageTopic> {
    private ClientProvider clientProvider;

    public PulsarTopicService(ClientProvider clientProvider) {
        this.clientProvider = clientProvider;
    }

    public void create(PulsarStorageTopic topic) {
        try {
            //TODO:: Check any other attributes to set on the topic e.g. retention, secure etc.
            clientProvider.getAdminClient().topics().createPartitionedTopic(topic.getName(), topic.getPartitionCount());
            log.info("Created the pulsar topic:{}", topic.getName());
        } catch (PulsarAdminException e) {
            if (e instanceof PulsarAdminException.ConflictException) {
                // No specific handling as of now.
                log.error("Topic {} already exists.", topic.getName());
            }
            throw new MessagingException(e);
        }
    }

    @Override
    public PulsarStorageTopic get(String topicName) {
        throw new NotImplementedException();
    }

}
