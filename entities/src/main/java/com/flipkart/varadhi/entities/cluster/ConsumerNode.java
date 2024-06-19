package com.flipkart.varadhi.entities.cluster;

import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import lombok.Getter;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import static java.util.Comparator.comparing;

@Getter
public class ConsumerNode {
    // Consumer Node info as viewed by Controller
    public static Comparator<ConsumerNode> NodeComparator = comparing(o -> o.available);
    private final String consumerId;
    private NodeCapacity available;
    private final Map<String, Assignment> assignments;
    private boolean markedForDeletion;

    public ConsumerNode(MemberInfo memberInfo) {
        this.consumerId = memberInfo.hostname();
        this.markedForDeletion = false;
        this.available = memberInfo.provisionedCapacity().clone();
        this.assignments = new HashMap<>();
    }

    public void markForDeletion() {
        this.markedForDeletion = true;
    }

    public void initFromConsumerInfo(ConsumerInfo consumerInfo) {
        available = consumerInfo.getAvailable().clone();
        assignments.clear();
        assignments.putAll(consumerInfo.getAssignments());
    }

    // allocate & free -- assumes single threaded caller.
    // They are being called from ShardAssigner.assignShard() and ShardAssigner.unAssignShard()
    // ShardAssigner is a single threaded executor.
    public void allocate(Assignment a, TopicCapacityPolicy requests) {
        if (null == assignments.putIfAbsent(a.getName(), a)) {
            available.allocate(requests);
        }
    }

    public void free(Assignment a, TopicCapacityPolicy requests) {
        if (null != assignments.remove(a.getName())) {
            available.free(requests);
        }
    }
}
