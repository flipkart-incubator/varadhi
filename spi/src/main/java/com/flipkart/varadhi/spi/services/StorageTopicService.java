package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.entities.InternalQueueCategory;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.TopicPartitions;

import java.util.List;

public interface StorageTopicService<T extends StorageTopic> {

    void create(T topic, Project project);

    List<TopicPartitions<T>> shardTopic(T topic, InternalQueueCategory category);

    void delete(String topicName, Project project);

    boolean exists(String topicName);
}
