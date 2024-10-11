package com.flipkart.varadhi.entities.cluster;

import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Consumer Info as maintained by the consumer node itself for tracking all assignments (and hence shards)
 * currently owned by it.
 */
@Getter
@AllArgsConstructor
public class ConsumerInfo {
    private final Map<String, Assignment> assignments;
    private String consumerId;
    private NodeCapacity available;

    public static ConsumerInfo from(MemberInfo memberInfo) {
        return new ConsumerInfo(
                new ConcurrentHashMap<>(), memberInfo.hostname(), memberInfo.provisionedCapacity().clone());
    }

    public void addShardCapacity(String subscriptionName, int shardId, TopicCapacityPolicy capacity) {
        Assignment assignment = new Assignment(subscriptionName, shardId, consumerId);
        if (null == assignments.putIfAbsent(assignment.getName(), assignment)) {
            available.allocate(capacity);
        }
    }

    public void freeShardCapacity(String subscriptionName, int shardId, TopicCapacityPolicy capacity) {
        Assignment assignment = new Assignment(subscriptionName, shardId, consumerId);
        if (null != assignments.remove(assignment.getName())) {
            available.free(capacity);
        }
    }
}
