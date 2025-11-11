package com.flipkart.varadhi.spi.mock;

import com.flipkart.varadhi.common.exceptions.DuplicateResourceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.entities.utils.TypeUtil;
import com.flipkart.varadhi.spi.services.StorageTopicService;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

public class InMemoryStorageTopicService implements StorageTopicService {

    private final ConcurrentHashMap<String, InMemoryStorageTopic> topics = new ConcurrentHashMap<>();

    @Override
    public void create(Project project, StorageTopic _topic) {
        var topic = TypeUtil.safeCast(_topic, InMemoryStorageTopic.class);
        if (topics.putIfAbsent(topic.getName(), topic) != null) {
            throw new DuplicateResourceException(String.format("StorageTopic(%s) already exists.", topic.getName()));
        }
    }

    @Override
    public List<TopicPartitions<? extends StorageTopic>> shardTopic(
        StorageTopic _topic,
        TopicCapacityPolicy capacity,
        InternalQueueCategory category
    ) {
        var topic = TypeUtil.safeCast(_topic, InMemoryStorageTopic.class);
        if (topic.getPartitions() == 1) {
            return List.of(TopicPartitions.byPartitions(topic, new int[] {0}));
        }
        int mid = topic.getPartitions() / 2;
        int[] firstHalf = IntStream.range(0, mid).toArray();
        int[] secondHalf = IntStream.range(mid, topic.getPartitions()).toArray();
        return List.of(TopicPartitions.byPartitions(topic, firstHalf), TopicPartitions.byPartitions(topic, secondHalf));
    }

    @Override
    public void delete(Project project, String topicName) {
        if (topics.remove(topicName) == null) {
            throw new ResourceNotFoundException(String.format("StorageTopic(%s) not found.", topicName));
        }
    }

    @Override
    public boolean exists(String topicName) {
        return topics.containsKey(topicName);
    }
}
