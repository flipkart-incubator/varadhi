package com.flipkart.varadhi.pulsar.consumer;

import com.flipkart.varadhi.pulsar.entities.PulsarOffset;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.spi.services.Consumer;
import com.flipkart.varadhi.spi.services.ConsumerFactory;
import com.flipkart.varadhi.spi.services.MessagingException;
import com.flipkart.varadhi.entities.TopicPartitions;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class PulsarConsumerFactory implements ConsumerFactory<PulsarStorageTopic, PulsarOffset> {

    private final PulsarClient pulsarClient;
    private final Map<String, Object> defaultConsumerProperties;

    public PulsarConsumerFactory(PulsarClient pulsarClient, Map<String, Object> defaultConsumerProperties) {
        this.pulsarClient = pulsarClient;
        this.defaultConsumerProperties = new HashMap<>(defaultConsumerProperties);

        // removing topicNames from defaultConsumerProperties as calling topics() method in newConsumer() method only
        // adds topics and not overrides it.
        this.defaultConsumerProperties.remove("topicNames");
    }

    @Override
    public Consumer<PulsarOffset> newConsumer(
            Collection<TopicPartitions<PulsarStorageTopic>> topics,
            String subscriptionName,
            String consumerName,
            Map<String, Object> properties
    ) throws MessagingException {

        try {

            Set<String> topicNames = new HashSet<>();

            for (TopicPartitions<PulsarStorageTopic> topic : topics) {
                if (!topic.hasSpecificPartitions()) {
                    topicNames.add(topic.getTopic().getName());
                } else {
                    for (int partition : topic.getPartitions()) {
                        String partitionName =
                                topic.getTopic().getName() + TopicName.PARTITIONED_TOPIC_SUFFIX + partition;
                        topicNames.add(TopicName.get(partitionName).toString());
                    }
                }
            }

            Map<String, Object> consumerProperties = new HashMap<>(defaultConsumerProperties);
            consumerProperties.putAll(properties);
            return new PulsarConsumer(
                    pulsarClient.newConsumer(Schema.BYTES)
                            .loadConf(consumerProperties)
                            .topics(new ArrayList<>(topicNames))
                            .subscriptionName(subscriptionName)
                            .consumerName(consumerName)
                            .poolMessages(true)
                            .batchReceivePolicy(BatchReceivePolicy.builder()
                                    .maxNumMessages(2000)
                                    .maxNumBytes(5 * 1024 * 1024)
                                    .timeout(200, TimeUnit.MILLISECONDS)
                                    .build())
                            .acknowledgmentGroupTime(1, TimeUnit.SECONDS)
                            .subscribe());
        } catch (PulsarClientException e) {
            throw new MessagingException("Error creating consumer", e);
        }
    }
}
