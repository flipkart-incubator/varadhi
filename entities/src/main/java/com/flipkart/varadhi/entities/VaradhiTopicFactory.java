package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.entities.*;

import java.util.List;
import java.util.stream.Collectors;

public class VaradhiTopicFactory {
    private final StorageTopicFactory<StorageTopic> topicFactory;

    public VaradhiTopicFactory(StorageTopicFactory<StorageTopic> topicFactory) {
        this.topicFactory = topicFactory;
    }

    public VaradhiTopic get(TopicResource topicResource) {
        VaradhiTopic vt = new VaradhiTopic(topicResource.getName(), topicResource.isGrouped(), topicResource.isExclusiveSubscription());
        planDeployment(vt, topicResource);
        return vt;
    }


    private void planDeployment(VaradhiTopic vt, TopicResource topicResource) {
        //create main topic.
        //TODO::instead of List, should be just one topic.
        List<InternalTopic> topics = InternalTopic.from(topicResource, topicFactory);
        vt.getInternalTopics().putAll(topics.stream().collect(Collectors.toMap(it -> it.getTopicKind(),  it -> it)));
    }
}
