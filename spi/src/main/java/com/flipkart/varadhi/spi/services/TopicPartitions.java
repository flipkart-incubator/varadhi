package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.entities.StorageTopic;

public class TopicPartitions<T extends StorageTopic> {

    public final T topic;
    public final int[] partitions;

    public TopicPartitions(T topic, int[] partitions) {
        this.topic = topic;
        this.partitions = partitions;
    }
}
