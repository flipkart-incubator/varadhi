package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.entities.BaseTopic;

public interface TopicService<T extends BaseTopic> {
    void create(T topic);

    T get(String topicName);
}
