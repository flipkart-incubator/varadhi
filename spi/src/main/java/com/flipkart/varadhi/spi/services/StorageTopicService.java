package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.entities.*;

import java.util.List;

public interface StorageTopicService {

    void create(Project project, StorageTopic topic, TopicCapacityPolicy capacityPolicy);

    List<TopicPartitions<? extends StorageTopic>> shardTopic(
        StorageTopic topic,
        TopicCapacityPolicy capacity,
        InternalQueueCategory category
    );

    void delete(Project project, String topicName);

    boolean exists(String topicName);

    /**
     * Replication backlog from {@code source} toward {@code target} for {@code topic}, in
     * backend-defined units (Pulsar: messages pending geo-replication). {@code 0} means caught up.
     *
     * <p>Default stub returns {@code 0} so topic-failover DRAIN can wire before a real stack
     * implementation lands. Pulsar should override with admin/replication metrics.
     */
    default long getReplicationLag(StorageTopic topic, RegionName source, RegionName target) {
        return 0L;
    }
}
