package com.flipkart.varadhi.db;

import com.flipkart.varadhi.entities.TopicResource;
import com.flipkart.varadhi.entities.VaradhiEntity;
import com.flipkart.varadhi.entities.VaradhiEntityType;
import com.flipkart.varadhi.entities.VaradhiTopic;

public interface MetaStore {

    TopicResource createTopicResource(TopicResource resource);

    boolean checkTopicResourceExists(String projectName, String topicName);

    TopicResource getTopicResource(String projectName, String resourceName);

    VaradhiTopic createVaradhiTopic(VaradhiTopic varadhiTopic);

    boolean checkVaradhiTopicExists(String varadhiTopicName);

    VaradhiTopic getVaradhiTopic(String topicName);

    VaradhiEntity createVaradhiEntity(VaradhiEntityType varadhiEntityType, VaradhiEntity varadhiEntity);

    boolean checkVaradhiEntityExists(VaradhiEntityType varadhiEntityType, String entityName);

    VaradhiEntity getVaradhiEntity(VaradhiEntityType varadhiEntityType,
                                   String entityName,
                                   Class<? extends VaradhiEntity> classType);
}
