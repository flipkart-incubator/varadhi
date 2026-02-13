package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.entities.*;

import java.util.List;

public interface StorageTopicService {

    void create(Project project, StorageTopic topic, TopicCapacityPolicy capacityPolicy);

    List<TopicPartitions<? extends StorageTopic>> shardTopic(
        StorageTopic topic,
        TopicCapacityPolicy capacity,
        InternalQueueCategory category
    );

    void delete(Project project, String topicName);

    boolean exists(String topicName);
}
