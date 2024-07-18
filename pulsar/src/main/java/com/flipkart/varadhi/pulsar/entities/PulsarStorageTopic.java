package com.flipkart.varadhi.pulsar.entities;

import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@EqualsAndHashCode(callSuper = true)
public class PulsarStorageTopic extends StorageTopic {
    private int partitionCount;

    private PulsarStorageTopic(String name, int version, int partitionCount, TopicCapacityPolicy capacity) {
        super(name, version, capacity);
        this.partitionCount = partitionCount;
    }

    public static PulsarStorageTopic of(String name, int partitionCount, TopicCapacityPolicy capacity) {
        return new PulsarStorageTopic(name, INITIAL_VERSION, partitionCount, capacity);
    }
}
