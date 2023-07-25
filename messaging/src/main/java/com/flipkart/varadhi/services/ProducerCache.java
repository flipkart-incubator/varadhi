package com.flipkart.varadhi.services;

import com.flipkart.varadhi.entities.Producer;
import com.flipkart.varadhi.entities.ProducerFactory;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.exceptions.VaradhiException;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.concurrent.ExecutionException;

public class ProducerCache {
    private final ProducerFactory producerFactory;
    Cache<String, Producer> producerCache;

    public ProducerCache(ProducerFactory producerFactory, String producerCacheBuilderSpec) {
        this.producerFactory = producerFactory;
        // If no specification, this will create a default cache of unlimited size and not expiring.
        this.producerCache = CacheBuilder.from(producerCacheBuilderSpec).build();
    }

    public Producer getProducer(StorageTopic storageTopic) {
        try {
            return this.producerCache.get(
                    storageTopic.getName(),
                    () -> producerFactory.getProducer(storageTopic)
            );
        } catch (ExecutionException e) {
            //TODO::evaluate if exception conversion is fine here ?
            throw new VaradhiException(e);
        }
    }
}
