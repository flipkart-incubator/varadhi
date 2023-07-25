package com.flipkart.varadhi.pulsar;

import com.flipkart.varadhi.entities.Producer;
import com.flipkart.varadhi.entities.ProducerFactory;
import com.flipkart.varadhi.pulsar.entities.PulsarProducer;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;

public class PulsarProducerFactory implements ProducerFactory<PulsarStorageTopic> {
    @Override
    public Producer getProducer(PulsarStorageTopic storageTopic) {
        return new PulsarProducer();
    }
}
