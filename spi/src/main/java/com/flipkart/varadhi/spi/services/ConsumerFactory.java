package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.entities.StorageTopic;

import java.util.Collection;

public interface ConsumerFactory<T extends StorageTopic, O extends Offset> {

    // TODO: need to see if we need property bag param or other params

    /**
     * @param topics to be consumed
     * @param name   name of the consumer. synonymous to consumer group / subscription name
     *
     * @return Consumer instance
     *
     * @throws MessagingException
     */
    Consumer<O> getConsumer(Collection<TopicPartitions<T>> topics, String name) throws MessagingException;
}
