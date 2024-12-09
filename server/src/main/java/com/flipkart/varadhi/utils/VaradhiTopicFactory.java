package com.flipkart.varadhi.utils;


import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.spi.services.StorageTopicFactory;
import com.flipkart.varadhi.entities.TopicResource;

public class VaradhiTopicFactory {

    private final StorageTopicFactory<StorageTopic> topicFactory;
    private final TopicCapacityPolicy defaultTopicCapacityPolicy;

    //TODO:: This is currently used to provide default value for primary region for the topic being created.
    //This should come from TopicResource a part of Regional/HA/BCP-DR policy. Since those are not available
    //use deploymentRegion as global single primary topic region as a workaround.
    private final String deploymentRegion;

    public VaradhiTopicFactory(
            StorageTopicFactory<StorageTopic> topicFactory, String deploymentRegion,
            TopicCapacityPolicy defaultTopicCapacityPolicy
    ) {
        this.topicFactory = topicFactory;
        this.defaultTopicCapacityPolicy = defaultTopicCapacityPolicy;
        this.deploymentRegion = deploymentRegion;
    }

    public VaradhiTopic get(Project project, TopicResource topicResource) {
        if (null == topicResource.getCapacity()) {
            topicResource.setCapacity(defaultTopicCapacityPolicy);
        }
        VaradhiTopic vt = VaradhiTopic.of(topicResource);
        planDeployment(project, vt);
        return vt;
    }

    private void planDeployment(Project project, VaradhiTopic varadhiTopic) {
        StorageTopic storageTopic =
                topicFactory.getTopic(
                        varadhiTopic.getName(), project, varadhiTopic.getCapacity(), InternalQueueCategory.MAIN);
        varadhiTopic.addInternalTopic(deploymentRegion, InternalCompositeTopic.of(storageTopic));
    }
}
