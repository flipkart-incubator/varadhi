package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.spi.services.Producer;
import com.flipkart.varadhi.spi.services.ProducerFactory;
import lombok.RequiredArgsConstructor;

/**
 * Producer boundary for consumer-owned retry and dead-letter topics.
 */
@RequiredArgsConstructor
public final class StorageTopicProducerService {

    private final ProducerFactory producerFactory;
    private final RegionName deployedRegion;

    public Producer<? extends Offset> getProducer(StorageTopic topic, TopicCapacityPolicy capacity) {
        return producerFactory.newProducer(topic, capacity, deployedRegion);
    }
}
