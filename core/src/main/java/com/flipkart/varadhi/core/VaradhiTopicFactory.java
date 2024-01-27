package com.flipkart.varadhi.core;


import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.spi.services.StorageTopicFactory;

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
        VaradhiTopic vt = VaradhiTopic.of(topicResource);
        planDeployment(project, vt);
        return vt;
    }


    private void planDeployment(Project project, VaradhiTopic varadhiTopic) {
        StorageTopic storageTopic =
                topicFactory.getTopic(varadhiTopic.getName(), project, varadhiTopic.getCapacityPolicy());
        InternalTopic internalTopic = new InternalTopic(
                deploymentRegion,
                TopicState.Producing,
                storageTopic
        );
        varadhiTopic.addInternalTopic(internalTopic);
    }
}
