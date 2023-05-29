package com.flipkart.varadhi.entities;


public class VaradhiTopicFactory {
    private static final String DEFAULT_TENANT = "public";
    private static final String DEFAULT_NAMESPACE = "default";
    private final StorageTopicFactory<StorageTopic> topicFactory;

    public VaradhiTopicFactory(StorageTopicFactory<StorageTopic> topicFactory) {
        this.topicFactory = topicFactory;
    }

    public VaradhiTopic get(TopicResource topicResource) {
        VaradhiTopic vt = new VaradhiTopic(varadhiTopicName(topicResource),
                topicResource.isGrouped(),
                topicResource.isExclusiveSubscription(),
                null);
        planDeployment(vt, topicResource);
        return vt;
    }


    private void planDeployment(VaradhiTopic varadhiTopic, TopicResource topicResource) {
        InternalTopic mainTopic = InternalTopic.from(varadhiTopic.getName(), topicResource, topicFactory);
        varadhiTopic.addInternalTopic(mainTopic);
    }

    private String varadhiTopicName(TopicResource topicResource) {
        //TODO::replace tenant and project
        //with input tenant and project from entities.
        return String.format("%s.%s.%s", DEFAULT_TENANT, DEFAULT_NAMESPACE, topicResource.getName());
    }
}
