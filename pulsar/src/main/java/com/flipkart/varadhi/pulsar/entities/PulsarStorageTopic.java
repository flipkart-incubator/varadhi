package com.flipkart.varadhi.pulsar.entities;

import com.flipkart.varadhi.Constants;
import com.flipkart.varadhi.entities.CapacityPolicy;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.StorageTopic;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.FieldDefaults;

@Getter
@EqualsAndHashCode(callSuper = true)
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class PulsarStorageTopic extends StorageTopic {

    private static final String TOPIC_SCHEMA = "persistent";
    int partitionCount;

    public PulsarStorageTopic(String name, int partitionCount) {
        super(name, Constants.INITIAL_VERSION);
        this.partitionCount = partitionCount;
    }

    public static PulsarStorageTopic of(
            Project project, String topicName, CapacityPolicy capacityPolicy
    ) {
        int partitionCount = getPartitionCount(capacityPolicy);
        String topicFqdn = generateFqdn(project, topicName);
        return new PulsarStorageTopic(topicFqdn, partitionCount);
    }

    private static int getPartitionCount(CapacityPolicy capacityPolicy) {
        //TODO::This should be based on capacity planner for the underlying messaging stack.
        return 1;
    }

    private static String generateFqdn(Project project, String topicName) {
        return String.format("%s://%s/%s/%s", TOPIC_SCHEMA, project.getTenantName(), project.getName(), topicName);
    }
}
