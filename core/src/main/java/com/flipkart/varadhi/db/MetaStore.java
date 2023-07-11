package com.flipkart.varadhi.db;

import com.flipkart.varadhi.entities.TopicResource;
import com.flipkart.varadhi.entities.VaradhiTopic;

import java.util.List;

public interface MetaStore {

    TopicResource createTopicResource(TopicResource resource);

    boolean checkTopicResourceExists(String projectName, String topicName);

    TopicResource getTopicResource(String projectName, String resourceName);

    VaradhiTopic createVaradhiTopic(VaradhiTopic varadhiTopic);

    boolean checkVaradhiTopicExists(String varadhiTopicName);

    VaradhiTopic getVaradhiTopic(String topicName);

    void deleteVaradhiTopic(String topicName);

    void deleteTopicResource(String projectName, String resourceName);

    List<String> listVaradhiTopics(String projectName);
}
