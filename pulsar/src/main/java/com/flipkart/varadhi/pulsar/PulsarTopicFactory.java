package com.flipkart.varadhi.pulsar;

import com.flipkart.varadhi.entities.InternalQueueCategory;
import com.flipkart.varadhi.entities.InternalQueueType;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.pulsar.util.TopicPlanner;
import com.flipkart.varadhi.spi.services.StorageTopicFactory;

public class PulsarTopicFactory implements StorageTopicFactory<PulsarStorageTopic> {
    private static final String TOPIC_SCHEMA = "persistent";
    private final TopicPlanner partitioner;

    public PulsarTopicFactory(TopicPlanner partitioner) {
        this.partitioner = partitioner;
    }

    private static String getPulsarTopicName(String topicName, Project project) {
        // persistent://<tenant>/<namespace>/topicName.
        return String.format("%s://%s/%s/%s", TOPIC_SCHEMA, project.getOrg(), project.getName(), topicName);
    }

    @Override
    public PulsarStorageTopic getTopic(
            String topicName,
            Project project,
            TopicCapacityPolicy capacity,
            InternalQueueCategory queueCategory
    ) {
        String pulsarTopicName = getPulsarTopicName(topicName, project);
        int partitionCount = partitioner.getPartitionCount(capacity, queueCategory);
        return PulsarStorageTopic.from(pulsarTopicName, partitionCount, capacity);
    }
}
