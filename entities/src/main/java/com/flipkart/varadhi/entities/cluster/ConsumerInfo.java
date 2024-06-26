package com.flipkart.varadhi.entities.cluster;

import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Getter
@AllArgsConstructor
public class ConsumerInfo {
    // Consumer Info as maintained by the consumer node itself.
    private String consumerId;
    private NodeCapacity available;
    private final Map<String, Assignment> assignments;

    public static ConsumerInfo from(MemberInfo memberInfo) {
        return new ConsumerInfo(memberInfo.hostname(), memberInfo.provisionedCapacity().clone(), new ConcurrentHashMap<>());
    }

    public void recordShardAssignment(String subscriptionName, int shardId, TopicCapacityPolicy capacity) {
        Assignment assignment = new Assignment(subscriptionName, shardId, consumerId);
        assignments.put(assignment.getName(), assignment);
        available.allocate(capacity);
    }

    public void purgeShardAssignment(String subscriptionName, int shardId, TopicCapacityPolicy capacity) {
        Assignment assignment = new Assignment(subscriptionName, shardId, consumerId);
        assignments.remove(assignment.getName());
        available.free(capacity);
    }
}
