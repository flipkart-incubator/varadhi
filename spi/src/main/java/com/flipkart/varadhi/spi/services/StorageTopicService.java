package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.StorageTopic;

public interface StorageTopicService<T extends StorageTopic> {

    void create(T topic, Project project);

    T get(String topicName);

    void delete(String topicName);

    boolean exists(String topicName);
}
