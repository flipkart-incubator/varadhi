package com.flipkart.varadhi.pulsar.entities;

import com.flipkart.varadhi.entities.StorageSubscription;
import com.flipkart.varadhi.entities.TopicPartitions;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode(callSuper = true)
public class PulsarSubscription extends StorageSubscription<PulsarStorageTopic> {
    public PulsarSubscription(String name, TopicPartitions<PulsarStorageTopic> topicPartitions) {
        super(name, topicPartitions);
    }
}
