package com.flipkart.varadhi.spi.db;

import com.flipkart.varadhi.entities.VaradhiTopic;

import java.util.List;

public interface TopicStore {
    void create(VaradhiTopic topic);

    VaradhiTopic get(String topicName);

    List<String> getAllNames(String projectName);

    List<VaradhiTopic> getAll();

    boolean exists(String topicName);

    void update(VaradhiTopic topic);

    void delete(String topicName);
}
