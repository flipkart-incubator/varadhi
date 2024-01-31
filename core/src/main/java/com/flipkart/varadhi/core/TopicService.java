package com.flipkart.varadhi.core;

import com.flipkart.varadhi.entities.AbstractTopic;
import com.flipkart.varadhi.entities.Project;

public interface TopicService<T extends AbstractTopic> {
    void create(T topic, Project project);

    T get(String topicName);

    void delete(String topicName);

    boolean exists(String topicName);
}
