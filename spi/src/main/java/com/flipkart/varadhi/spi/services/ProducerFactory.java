package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;

public interface ProducerFactory {
    /**
     * Creates a producer for {@code storageTopic} in {@code produceRegion}.
     *
     * <p>The region is part of the broker producer name so multi-region warms on a shared broker
     * do not collide.
     */
    Producer<? extends Offset> newProducer(
        StorageTopic storageTopic,
        TopicCapacityPolicy capacity,
        RegionName produceRegion
    ) throws MessagingException;
}
