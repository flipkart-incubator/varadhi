package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.StorageSubscription;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.TopicPartitions;

public interface StorageSubscriptionFactory<S extends StorageSubscription<? extends StorageTopic>> {
    S get(String subName, TopicPartitions<? extends StorageTopic> topicPartitions, Project project);
}
