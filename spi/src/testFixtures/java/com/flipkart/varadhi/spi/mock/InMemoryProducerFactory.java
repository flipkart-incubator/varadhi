package com.flipkart.varadhi.spi.mock;

import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.spi.services.MessagingException;
import com.flipkart.varadhi.spi.services.Producer;
import com.flipkart.varadhi.spi.services.ProducerFactory;
import io.netty.util.HashedWheelTimer;

public class InMemoryProducerFactory implements ProducerFactory<InMemoryStorageTopic> {

    private final HashedWheelTimer timer = new HashedWheelTimer();

    @Override
    public Producer newProducer(InMemoryStorageTopic storageTopic, TopicCapacityPolicy capacity)
        throws MessagingException {
        return new InMemoryProducer(storageTopic, true, timer, true);
    }
}
