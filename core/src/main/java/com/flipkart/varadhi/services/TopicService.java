package com.flipkart.varadhi.services;

import com.flipkart.varadhi.entities.BaseTopic;

public interface TopicService <T extends BaseTopic> {
    void create(T topic);
}
