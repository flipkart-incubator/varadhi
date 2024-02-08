package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.entities.StorageTopic;

public class TopicPartitions<T extends StorageTopic> {

    public final T topic;
    public final int[] partition;

    public TopicPartitions(T topic, int[] partition) {
        this.topic = topic;
        this.partition = partition;
    }
}
