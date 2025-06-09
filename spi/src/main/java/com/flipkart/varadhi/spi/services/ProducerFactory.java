package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;

public interface ProducerFactory<T extends StorageTopic> {
    Producer newProducer(T storageTopic, TopicCapacityPolicy capacity) throws MessagingException;
}
