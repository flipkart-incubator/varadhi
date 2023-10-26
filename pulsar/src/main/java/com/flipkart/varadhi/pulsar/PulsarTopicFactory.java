package com.flipkart.varadhi.pulsar;

import com.flipkart.varadhi.entities.CapacityPolicy;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.spi.services.StorageTopicFactory;

public class PulsarTopicFactory implements StorageTopicFactory<PulsarStorageTopic> {

    private static final String TOPIC_SCHEMA = "persistent";


    @Override
    public PulsarStorageTopic getTopic(
            String topicName,
            Project project,
            CapacityPolicy capacityPolicy
    ) {
        String pulsarTopicName = getPulsarTopicName(topicName, project);
        return PulsarStorageTopic.from(pulsarTopicName, capacityPolicy);
    }

    private static String getPulsarTopicName(String topicName, Project project) {
        // persistent://<tenant>/<namespace>/topicName.
        return String.format("%s://%s/%s/%s", TOPIC_SCHEMA, project.getOrg(), project.getName(), topicName);
    }

}
