package com.flipkart.varadhi.pulsar.consumer;

import com.flipkart.varadhi.pulsar.config.ConsumerOptions;
import com.flipkart.varadhi.pulsar.config.TelemetryOptions;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.spi.services.Consumer;
import com.flipkart.varadhi.spi.services.ConsumerFactory;
import com.flipkart.varadhi.spi.services.MessagingException;
import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.TopicPartitions;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.common.naming.TopicName;

import java.util.*;

public class PulsarConsumerFactory implements ConsumerFactory {

    private final PulsarClient pulsarClient;
    private final TelemetryOptions telemetryOptions;
    private final ConsumerOptions consumerOptions;

    public PulsarConsumerFactory(
        PulsarClient pulsarClient,
        ConsumerOptions defaultConsumerProperties,
        TelemetryOptions telemetryOptions
    ) {
        this.pulsarClient = pulsarClient;
        this.telemetryOptions = telemetryOptions;
        this.consumerOptions = defaultConsumerProperties;
    }

    @Override
    public Consumer<? extends Offset> newConsumer(
        Collection<TopicPartitions<? extends StorageTopic>> _topics,
        String subscriptionName,
        String consumerName,
        Map<String, Object> properties
    ) throws MessagingException {

        try {

            Set<String> topicNames = new HashSet<>();

            for (var _topic : _topics) {
                TopicPartitions<PulsarStorageTopic> topic = _topic.lift(PulsarStorageTopic.class);
                if (!topic.hasSpecificPartitions()) {
                    topicNames.add(topic.getTopic().getName());
                } else {
                    for (int partition : topic.getPartitions()) {
                        String partitionName = topic.getTopic().getName() + TopicName.PARTITIONED_TOPIC_SUFFIX
                                               + partition;
                        topicNames.add(TopicName.get(partitionName).toString());
                    }
                }
            }

            return new PulsarConsumer(
                pulsarClient,
                consumerOptions,
                topicNames,
                subscriptionName,
                consumerName,
                telemetryOptions
            );
        } catch (PulsarClientException e) {
            throw new MessagingException("Error creating consumer", e);
        }
    }

}
