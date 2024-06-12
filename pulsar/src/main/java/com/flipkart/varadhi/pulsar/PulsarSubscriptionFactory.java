package com.flipkart.varadhi.pulsar;

import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.TopicPartitions;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.pulsar.entities.PulsarSubscription;
import com.flipkart.varadhi.spi.services.StorageSubscriptionFactory;

public class PulsarSubscriptionFactory implements StorageSubscriptionFactory<PulsarSubscription, PulsarStorageTopic> {

    @Override
    public PulsarSubscription get(
            String subName, TopicPartitions<PulsarStorageTopic> topicPartitions, Project project
    ) {
        return new PulsarSubscription(subName, topicPartitions);
    }
}
