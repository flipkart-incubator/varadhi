package com.flipkart.varadhi.core.topic;

import com.flipkart.varadhi.entities.InternalQueueCategory;
import com.flipkart.varadhi.entities.MessageSizeProfile;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.RateLimiterMode;
import com.flipkart.varadhi.entities.SegmentedStorageTopic;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.web.TopicResource;
import com.flipkart.varadhi.spi.services.StorageTopicFactory;

import java.util.Optional;
import java.util.Set;

/**
 * Factory class for creating Varadhi topics.
 */
public class VaradhiTopicFactory {

    private final StorageTopicFactory<? extends StorageTopic> topicFactory;
    private final TopicCapacityPolicy defaultTopicCapacityPolicy;
    private final RateLimiterMode defaultRateLimiterMode;

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
        StorageTopicFactory<? extends StorageTopic> topicFactory,
        String deploymentRegion,
        TopicCapacityPolicy defaultTopicCapacityPolicy
    ) {
        this(topicFactory, deploymentRegion, defaultTopicCapacityPolicy, RateLimiterMode.disabled);
    }

    public VaradhiTopicFactory(
        StorageTopicFactory<? extends StorageTopic> topicFactory,
        String deploymentRegion,
        TopicCapacityPolicy defaultTopicCapacityPolicy,
        RateLimiterMode defaultRateLimiterMode
    ) {
        this.topicFactory = topicFactory;
        this.defaultTopicCapacityPolicy = defaultTopicCapacityPolicy;
        this.deploymentRegion = deploymentRegion;
        this.defaultRateLimiterMode = defaultRateLimiterMode;
    }

    /**
     * Creates a VaradhiTopic instance based on the provided project, topic resource, and topic category.
     *
     * @param project       the project associated with the topic
     * @param topicResource the topic model containing topic details
     * @param category      the topic category (e.g. {@link VaradhiTopic.TopicCategory#TOPIC} or {@link VaradhiTopic.TopicCategory#QUEUE})
     *
     * @return the created VaradhiTopic instance
     */
    public VaradhiTopic get(Project project, TopicResource topicResource, VaradhiTopic.TopicCategory category) {
        topicResource.setCapacity(
            Optional.ofNullable(topicResource.getCapacity()).orElse(defaultTopicCapacityPolicy)
        );

        topicResource.setRateLimiterMode(
            Optional.ofNullable(topicResource.getRateLimiterMode()).orElse(defaultRateLimiterMode)
        );

        MessageSizeProfile messageSizeProfile = topicResource.getMessageSizeProfile();
        if (messageSizeProfile != null) {
            TopicCapacityConsistencyValidator.validate(topicResource.getCapacity(), messageSizeProfile);
        }

        topicResource.setPerRegionQuotaWeights(
            PerRegionQuotaWeightsResolver.resolve(
                topicResource.getPerRegionQuotaWeights(),
                Set.of(deploymentRegion)
            )
        );

        VaradhiTopic varadhiTopic = topicResource.toVaradhiTopic(category);
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
            0,
            varadhiTopic.getName(),
            project,
            varadhiTopic.getCapacity(),
            InternalQueueCategory.MAIN
        );

        varadhiTopic.addInternalTopic(deploymentRegion, SegmentedStorageTopic.of(storageTopic));
    }
}
