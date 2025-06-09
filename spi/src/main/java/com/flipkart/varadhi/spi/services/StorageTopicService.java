package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.entities.*;

import java.util.List;

public interface StorageTopicService<T extends StorageTopic> {

    void create(T topic, Project project);

    List<TopicPartitions<T>> shardTopic(T topic, TopicCapacityPolicy capacity, InternalQueueCategory category);

    void delete(String topicName, Project project);

    boolean exists(String topicName);
}
