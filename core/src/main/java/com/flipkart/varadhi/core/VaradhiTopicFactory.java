package com.flipkart.varadhi.core;


import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.spi.services.StorageTopicFactory;

import static com.flipkart.varadhi.Constants.NAME_SEPARATOR;

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
        planDeployment(project, vt, topicResource);
        return vt;
    }


    private void planDeployment(Project project, VaradhiTopic varadhiTopic, TopicResource topicResource) {
        CapacityPolicy capacityPolicy = topicResource.getCapacityPolicy();
        if (null == capacityPolicy) {
            capacityPolicy = getDefaultCapacityPolicy();
        }
        StorageTopic storageTopic =
                topicFactory.getTopic(varadhiTopic.getName(), project, capacityPolicy);
        // This is likely to change with replicated topics across zones. To be taken care as part of DR.
        String internalTopicName = String.join(NAME_SEPARATOR, varadhiTopic.getName(), deploymentRegion);
        InternalTopic internalTopic = new InternalTopic(
                internalTopicName,
                deploymentRegion,
                TopicState.Producing,
                storageTopic
        );
        varadhiTopic.addInternalTopic(internalTopic);
    }

    private CapacityPolicy getDefaultCapacityPolicy() {
        //TODO:: make default capacity config based instead of hard coding.
        return CapacityPolicy.getDefault();
    }
}
