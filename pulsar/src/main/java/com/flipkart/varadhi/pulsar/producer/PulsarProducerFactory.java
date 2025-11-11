package com.flipkart.varadhi.pulsar.producer;

import com.flipkart.varadhi.common.exceptions.ProduceException;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.entities.utils.TypeUtil;
import com.flipkart.varadhi.pulsar.config.ProducerOptions;
import com.flipkart.varadhi.pulsar.entities.PulsarOffset;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.spi.services.Producer;
import com.flipkart.varadhi.spi.services.ProducerFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;


@Slf4j
public class PulsarProducerFactory implements ProducerFactory {
    private final PulsarClient pulsarClient;
    private final String hostName;
    private final ProducerOptions producerOptions;


    public PulsarProducerFactory(PulsarClient pulsarClient, ProducerOptions producerOptions, String hostName) {
        this.pulsarClient = pulsarClient;
        this.hostName = hostName;
        this.producerOptions = null == producerOptions ? new ProducerOptions() : producerOptions;
    }

    @Override
    public Producer<PulsarOffset> newProducer(StorageTopic _topic, TopicCapacityPolicy capacity) {
        var topic = TypeUtil.safeCast(_topic, PulsarStorageTopic.class);
        try {
            return new PulsarProducer(pulsarClient, topic, capacity, producerOptions, hostName);
        } catch (PulsarClientException e) {
            throw new ProduceException(
                String.format("Failed to create Pulsar producer for %s. %s", topic.getName(), e.getMessage()),
                e
            );
        }
    }
}
