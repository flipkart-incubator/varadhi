package com.flipkart.varadhi.pulsar.entities;

import com.flipkart.varadhi.Constants;
import com.flipkart.varadhi.entities.CapacityPolicy;
import com.flipkart.varadhi.entities.StorageTopic;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.FieldDefaults;

@Getter
@EqualsAndHashCode(callSuper = true)
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class PulsarStorageTopic extends StorageTopic {
    private static final String DEFAULT_TENANT = "public";
    private static final String DEFAULT_NAMESPACE = "default";
    private static final String TOPIC_SCHEMA = "persistent";
    int partitionCount;
    String namespace;

    public PulsarStorageTopic(String name, int partitionCount) {
        super(name, Constants.INITIAL_VERSION);
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

    public String getFqdn() {
        //TODO::tenant and namespace should be assigned when creating PulsarStorageTopic.
        return String.format("%s://%s/%s/%s", TOPIC_SCHEMA, DEFAULT_TENANT, DEFAULT_NAMESPACE, getName());
    }
}
