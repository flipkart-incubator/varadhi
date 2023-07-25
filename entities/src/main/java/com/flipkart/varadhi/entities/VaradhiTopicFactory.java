package com.flipkart.varadhi.entities;


import com.flipkart.varadhi.Constants;

public class VaradhiTopicFactory {

    private final StorageTopicFactory<StorageTopic> topicFactory;

    public VaradhiTopicFactory(StorageTopicFactory<StorageTopic> topicFactory) {
        this.topicFactory = topicFactory;
    }

    public VaradhiTopic get(Project project, TopicResource topicResource) {
        VaradhiTopic vt = new VaradhiTopic(
                getVaradhiTopicName(topicResource),
                Constants.INITIAL_VERSION,
                topicResource.isGrouped(),
                topicResource.isExclusiveSubscription(),
                null
        );
        planDeployment(project, vt, topicResource);
        return vt;
    }


    private void planDeployment(Project project, VaradhiTopic varadhiTopic, TopicResource topicResource) {
        InternalTopic mainTopic =
                InternalTopic.from(project, varadhiTopic.getName(), topicResource, topicFactory);
        varadhiTopic.addInternalTopic(mainTopic);
    }

    private String getVaradhiTopicName(TopicResource topicResource) {
        return VaradhiTopic.getTopicFQN(topicResource.getProject(), topicResource.getName());
    }
}
