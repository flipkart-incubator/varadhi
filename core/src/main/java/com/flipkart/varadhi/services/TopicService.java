package com.flipkart.varadhi.services;

import com.flipkart.varadhi.entities.VaradhiResource;

public interface TopicService<T extends VaradhiResource> {
    void create(T topic);
}
