package com.flipkart.varadhi.pulsar;

import com.flipkart.varadhi.pulsar.entities.PulsarProducer;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.spi.services.Producer;
import com.flipkart.varadhi.spi.services.ProducerFactory;

public class PulsarProducerFactory implements ProducerFactory<PulsarStorageTopic> {
    @Override
    public Producer getProducer(PulsarStorageTopic storageTopic) {
        return new PulsarProducer();
    }
}
