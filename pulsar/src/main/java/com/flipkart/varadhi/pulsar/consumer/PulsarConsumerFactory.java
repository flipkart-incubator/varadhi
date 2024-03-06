package com.flipkart.varadhi.pulsar.consumer;

import com.flipkart.varadhi.pulsar.entities.PulsarOffset;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.spi.services.Consumer;
import com.flipkart.varadhi.spi.services.ConsumerFactory;
import com.flipkart.varadhi.spi.services.MessagingException;
import com.flipkart.varadhi.spi.services.TopicPartitions;
import lombok.RequiredArgsConstructor;
import org.apache.pulsar.client.api.PulsarClient;

import java.util.Collection;

@RequiredArgsConstructor
public class PulsarConsumerFactory implements ConsumerFactory<PulsarStorageTopic, PulsarOffset> {

    private final PulsarClient pulsarClient;


    @Override
    public Consumer<PulsarOffset> getConsumer(Collection<TopicPartitions<PulsarStorageTopic>> topics, String name)
            throws MessagingException {
        return null;
    }
}
