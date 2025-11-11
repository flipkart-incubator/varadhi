package com.flipkart.varadhi.spi.mock;

import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.entities.utils.TypeUtil;
import com.flipkart.varadhi.spi.services.MessagingException;
import com.flipkart.varadhi.spi.services.Producer;
import com.flipkart.varadhi.spi.services.ProducerFactory;
import io.netty.util.HashedWheelTimer;

public class InMemoryProducerFactory implements ProducerFactory {

    private final HashedWheelTimer timer = new HashedWheelTimer();

    public InMemoryProducerFactory() {
        Runtime.getRuntime().addShutdownHook(new Thread(timer::stop));
    }

    @Override
    public Producer<InMemoryOffset> newProducer(StorageTopic _topic, TopicCapacityPolicy capacity)
        throws MessagingException {
        var topic = TypeUtil.safeCast(_topic, InMemoryStorageTopic.class);
        return new InMemoryProducer(topic, true, timer, true);
    }
}
