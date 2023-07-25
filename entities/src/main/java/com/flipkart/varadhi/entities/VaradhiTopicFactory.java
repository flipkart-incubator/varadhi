package com.flipkart.varadhi.entities;


import com.flipkart.varadhi.Constants;

public class VaradhiTopicFactory {

    private final StorageTopicFactory<StorageTopic> topicFactory;

    //TODO:: This is currently used to provide default value for primary region for the topic being created.
    //This should come from TopicResource a part of Regional/HA/BCP-DR policy. Since those are not available
    //use deploymentRegion as global single primary topic region as a workaround.
    private final String deploymentRegion;

    public VaradhiTopicFactory(StorageTopicFactory<StorageTopic> topicFactory, String deploymentRegion) {
        this.topicFactory = topicFactory;
        this.deploymentRegion = deploymentRegion;
    }

    public VaradhiTopic get(Project project, TopicResource topicResource) {
        VaradhiTopic vt = new VaradhiTopic(
                getVaradhiTopicName(topicResource),
                Constants.INITIAL_VERSION,
                topicResource.isGrouped(),
                null
        );
        planDeployment(project, vt, topicResource);
        return vt;
    }


    private void planDeployment(Project project, VaradhiTopic varadhiTopic, TopicResource topicResource) {
        InternalTopic mainTopic =
                InternalTopic.mainTopicFrom(
                        project, varadhiTopic.getName(), deploymentRegion, topicResource, topicFactory);
        varadhiTopic.addInternalTopic(mainTopic);
    }

    private String getVaradhiTopicName(TopicResource topicResource) {
        return VaradhiTopic.getTopicFQN(topicResource.getProject(), topicResource.getName());
    }
}
