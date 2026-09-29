package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;

public interface ProducerFactory {
    /**
     * Creates a producer for {@code storageTopic}.
     *
     * @param produceRegion logical produce region (used in Pulsar producer naming so multi-region
     *                      warms on a shared broker do not collide)
     */
    Producer<? extends Offset> newProducer(
        StorageTopic storageTopic,
        TopicCapacityPolicy capacity,
        String produceRegion
    ) throws MessagingException;
}
