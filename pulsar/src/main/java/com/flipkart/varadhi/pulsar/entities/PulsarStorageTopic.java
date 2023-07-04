package com.flipkart.varadhi.pulsar.entities;

import com.flipkart.varadhi.Constants;
import com.flipkart.varadhi.entities.CapacityPolicy;
import com.flipkart.varadhi.entities.InternalTopic;
import com.flipkart.varadhi.entities.StorageTopic;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.FieldDefaults;

@Getter
@EqualsAndHashCode(callSuper = true)
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class PulsarStorageTopic extends StorageTopic {
    int partitionCount;
    String namespace;

    public PulsarStorageTopic(String name, int partitionCount) {
        super(InternalTopic.StorageKind.Pulsar, name, Constants.INITIAL_VERSION);
        this.partitionCount = partitionCount;
        this.namespace = "default";
    }

    public static PulsarStorageTopic of(String name, CapacityPolicy capacityPolicy) {
        int partitionCount = getPartitionCount(capacityPolicy);
        return new PulsarStorageTopic(name, partitionCount);
    }

    private static int getPartitionCount(CapacityPolicy capacityPolicy) {
        //This should be based on capacity planner for the underlying messaging stack.
        return 1;
    }
}
