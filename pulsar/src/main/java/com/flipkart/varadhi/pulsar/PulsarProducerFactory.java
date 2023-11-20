package com.flipkart.varadhi.pulsar;

import com.flipkart.varadhi.exceptions.ProduceException;
import com.flipkart.varadhi.pulsar.clients.ClientProvider;
import com.flipkart.varadhi.pulsar.config.ProducerOptions;
import com.flipkart.varadhi.pulsar.entities.PulsarProducer;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.spi.services.Producer;
import com.flipkart.varadhi.spi.services.ProducerFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClientException;


@Slf4j
public class PulsarProducerFactory implements ProducerFactory<PulsarStorageTopic> {
    private final ClientProvider clientProvider;
    private final String hostName;
    private final ProducerOptions producerOptions;


    public PulsarProducerFactory(ClientProvider clientProvider, ProducerOptions producerOptions, String hostName) {
        this.clientProvider = clientProvider;
        this.hostName = hostName;
        this.producerOptions = null == producerOptions ? new ProducerOptions() : producerOptions;
    }

    @Override
    public Producer getProducer(PulsarStorageTopic storageTopic) {
        try {
            return new PulsarProducer(clientProvider, storageTopic, producerOptions, hostName);
        } catch (PulsarClientException e) {
            throw new ProduceException(
                    String.format(
                            "Failed to create Pulsar producer for %s. %s", storageTopic.getName(), e.getMessage()), e);
        }
    }
}
