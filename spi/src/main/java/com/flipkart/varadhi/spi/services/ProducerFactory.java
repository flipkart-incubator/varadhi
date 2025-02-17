package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.config.MessageHeaderConfiguration;

public interface ProducerFactory<T extends StorageTopic> {
    Producer newProducer(T storageTopic) throws MessagingException;
}
