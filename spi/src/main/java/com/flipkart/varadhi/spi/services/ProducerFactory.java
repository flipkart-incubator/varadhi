package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;

public interface ProducerFactory {
    Producer<? extends Offset> newProducer(StorageTopic storageTopic, TopicCapacityPolicy capacity)
        throws MessagingException;
}
