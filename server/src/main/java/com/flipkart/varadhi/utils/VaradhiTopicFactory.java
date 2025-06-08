package com.flipkart.varadhi.utils;

import com.flipkart.varadhi.entities.SegmentedStorageTopic;
import com.flipkart.varadhi.entities.InternalQueueCategory;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.spi.services.StorageTopicFactory;
import com.flipkart.varadhi.web.entities.TopicResource;

import java.util.Optional;

/**
 * Factory class for creating Varadhi topics.
 */
public class VaradhiTopicFactory {

    private final StorageTopicFactory<StorageTopic> topicFactory;
    private final TopicCapacityPolicy defaultTopicCapacityPolicy;

    /**
     * TODO: This field is currently used to provide a default value for the primary region of the topic being created.
     * Ideally, this should be derived from the TopicResource as part of the Regional/HA/BCP-DR policy.
     * Since those policies are not yet available, the deploymentRegion is used as a global single primary
     * topic region as a temporary solution.
     */
    private final String deploymentRegion;

    /**
     * Constructs a new VaradhiTopicFactory instance.
     *
     * @param topicFactory               the factory for creating storage topics
     * @param deploymentRegion           the default primary region for the topic
     * @param defaultTopicCapacityPolicy the default capacity policy for the topic
     */
    public VaradhiTopicFactory(
        StorageTopicFactory<StorageTopic> topicFactory,
        String deploymentRegion,
        TopicCapacityPolicy defaultTopicCapacityPolicy
    ) {
        this.topicFactory = topicFactory;
        this.defaultTopicCapacityPolicy = defaultTopicCapacityPolicy;
        this.deploymentRegion = deploymentRegion;
    }

    /**
     * Creates a VaradhiTopic instance based on the provided project and topic resource.
     *
     * @param project       the project associated with the topic
     * @param topicResource the topic resource containing topic details
     *
     * @return the created VaradhiTopic instance
     */
    public VaradhiTopic get(Project project, TopicResource topicResource) {
        topicResource.setCapacity(Optional.ofNullable(topicResource.getCapacity()).orElse(defaultTopicCapacityPolicy));

        VaradhiTopic varadhiTopic = topicResource.toVaradhiTopic();
        planDeployment(project, varadhiTopic);
        return varadhiTopic;
    }

    /**
     * Plans the deployment of the VaradhiTopic by creating and associating an internal storage topic.
     *
     * @param project      the project associated with the topic
     * @param varadhiTopic the VaradhiTopic instance to be deployed
     */
    private void planDeployment(Project project, VaradhiTopic varadhiTopic) {
        StorageTopic storageTopic = topicFactory.getTopic(
            varadhiTopic.getName(),
            project,
            varadhiTopic.getCapacity(),
            InternalQueueCategory.MAIN
        );

        varadhiTopic.addInternalTopic(deploymentRegion, SegmentedStorageTopic.of(storageTopic));
    }
}
