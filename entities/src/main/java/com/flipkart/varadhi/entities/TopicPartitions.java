package com.flipkart.varadhi.entities;

import lombok.Getter;

@Getter
public class TopicPartitions<T extends StorageTopic> {
    private final T topic;
    private final int[] partitions;

    private TopicPartitions(T topic, int[] partitions) {
        this.topic = topic;
        this.partitions = partitions;
    }

    public static <T extends StorageTopic> TopicPartitions<T> byPartitions(T topic, int[] partitions) {
        if (partitions == null || partitions.length == 0) {
            throw new IllegalArgumentException("partitions array is null / empty");
        }
        return new TopicPartitions<>(topic, partitions);
    }

    public static <T extends StorageTopic> TopicPartitions<T> byTopic(T topic) {
        return new TopicPartitions<>(topic, null);
    }

    public boolean hasSpecificPartitions() {
        return partitions != null;
    }
}
