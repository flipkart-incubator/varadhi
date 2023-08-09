package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.entities.VaradhiResource;

public interface TopicService<T extends VaradhiResource> {
    void create(T topic);

    T get(String topicName);
}
