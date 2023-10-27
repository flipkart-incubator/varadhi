package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.exceptions.MessagingException;

public interface ProducerFactory<T extends StorageTopic> {
    Producer getProducer(T storageTopic) throws MessagingException;
}
