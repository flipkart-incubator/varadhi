package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.StorageSubscription;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.TopicPartitions;

//TODO::Fix warning, raw usage of StorageSubscription
public interface StorageSubscriptionFactory<S extends StorageSubscription, T extends StorageTopic>  {
    S get(String subName, TopicPartitions<T> topicPartitions, Project project);

}
