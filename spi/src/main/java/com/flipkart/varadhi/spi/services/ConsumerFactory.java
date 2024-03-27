package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.entities.StorageTopic;

import java.util.Collection;
import java.util.Map;

public interface ConsumerFactory<T extends StorageTopic, O extends Offset> {

    /**
     * Retrieves a consumer instance for consuming messages from given topics.
     *
     * @param topics     The topics to be consumed.
     * @param subscriptionName The name of the consumer, synonymous with consumer group or subscription name.
     * @param consumerName The name of this consumer instance.
     * @param properties The properties to be used for creating the consumer. The impl needs to use reasonable defaults,
     *                   but provided properties allows for overriding those defaults.
     *
     * @return A Consumer instance.
     *
     * @throws MessagingException if an error occurs while creating the consumer.
     */
    Consumer<O> newConsumer(
            Collection<TopicPartitions<T>> topics,
            String subscriptionName,
            String consumerName,
            Map<String, Object> properties
    ) throws MessagingException;
}
