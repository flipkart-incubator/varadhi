package com.flipkart.varadhi.spi.mock;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.flipkart.varadhi.entities.StorageSubscription;
import com.flipkart.varadhi.entities.TopicPartitions;

@JsonTypeName ("in-memory-storage-sub")
public class InMemoryStorageSubscription extends StorageSubscription<InMemoryStorageTopic> {

    public InMemoryStorageSubscription(String name, TopicPartitions<InMemoryStorageTopic> topicPartitions) {
        super(name, topicPartitions);
    }
}
