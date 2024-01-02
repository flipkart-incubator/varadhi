package com.flipkart.varadhi.pulsar.entities;

import com.flipkart.varadhi.entities.CapacityPolicy;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.VersionedEntity;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@EqualsAndHashCode(callSuper = true)
public class PulsarStorageTopic extends StorageTopic {

    private int partitionCount;

    private int maxQPS;

    private int maxThroughputKBps;

    private PulsarStorageTopic(String name, int version, int partitionCount, int maxQPS, int maxThroughputKBps) {
        super(name, version);
        this.partitionCount = partitionCount;
        this.maxQPS = maxQPS;
        this.maxThroughputKBps = maxThroughputKBps;
    }

    public static PulsarStorageTopic from(String name, CapacityPolicy capacityPolicy) {
        return new PulsarStorageTopic(name, INITIAL_VERSION, getPartitionCount(capacityPolicy),
                capacityPolicy.getMaxQPS(), capacityPolicy.getMaxThroughputKBps()
        );
    }

    private static int getPartitionCount(CapacityPolicy capacityPolicy) {
        //TODO::This should be based on capacity planner for the underlying messaging stack.
        return 1;
    }

}
