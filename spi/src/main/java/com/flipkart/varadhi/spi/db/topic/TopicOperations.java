package com.flipkart.varadhi.spi.db.topic;

import com.flipkart.varadhi.entities.VaradhiTopic;

import java.util.List;

public interface TopicOperations {
    void createTopic(VaradhiTopic topic);
    VaradhiTopic getTopic(String topicName);
    List<String> getTopicNames(String projectName);
    boolean checkTopicExists(String topicName);
    void updateTopic(VaradhiTopic topic);
    void deleteTopic(String topicName);
}