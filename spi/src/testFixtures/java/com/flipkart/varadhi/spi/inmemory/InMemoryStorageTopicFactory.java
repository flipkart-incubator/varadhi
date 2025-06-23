package com.flipkart.varadhi.spi.inmemory;

import com.flipkart.varadhi.entities.InternalQueueCategory;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.spi.services.StorageTopicFactory;

import java.util.concurrent.ConcurrentHashMap;

public class InMemoryStorageTopicFactory implements StorageTopicFactory<InMemoryStorageTopic> {
    private final ConcurrentHashMap<String, InMemoryStorageTopic> topics = new ConcurrentHashMap<>();

    @Override
    public InMemoryStorageTopic getTopic(
        int id,
        String topicName,
        Project project,
        TopicCapacityPolicy capacity,
        InternalQueueCategory queueCategory
    ) {
        return topics.computeIfAbsent(topicName, name -> new InMemoryStorageTopic(id, name, 2));
    }
}
