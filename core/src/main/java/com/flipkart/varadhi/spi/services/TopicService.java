package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.entities.AbstractTopic;

public interface TopicService<T extends AbstractTopic> {
    void create(T topic, String orgName, String projectName);

    T get(String topicName);

    void delete(T topic);
}
