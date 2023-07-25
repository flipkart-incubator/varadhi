package com.flipkart.varadhi.entities;

public interface ProducerFactory<T extends StorageTopic> {
    Producer getProducer(T storageTopic);
}
