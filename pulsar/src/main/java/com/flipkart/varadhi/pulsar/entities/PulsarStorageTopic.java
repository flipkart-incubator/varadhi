package com.flipkart.varadhi.pulsar.entities;

import com.flipkart.varadhi.entities.StorageTopic;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@EqualsAndHashCode (callSuper = true)
public class PulsarStorageTopic extends StorageTopic {
    private int partitionCount;

    private PulsarStorageTopic(int id, String name, int partitionCount) {
        super(id, name);
        this.partitionCount = partitionCount;
    }

    public static PulsarStorageTopic of(int id, String name, int partitionCount) {
        return new PulsarStorageTopic(id, name, partitionCount);
    }
}
