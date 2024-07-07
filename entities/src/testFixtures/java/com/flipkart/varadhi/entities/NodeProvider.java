package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.entities.cluster.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class NodeProvider {

    public static List<ConsumerNode> getConsumerNodes(int numNodes) {
        return getConsumerNodes(numNodes, getNodeCapacity(5000, 100000));
    }
    public static List<ConsumerNode> getConsumerNodes(int numNodes, NodeCapacity capacity) {
        List<ConsumerNode> nodes = new ArrayList<>();
        for (int i = 0; i < numNodes; i++) {
            nodes.add(new ConsumerNode(new MemberInfo("test.consumer-node." + i, 0, new ComponentKind[]{ComponentKind.Consumer}, capacity)));
        }
        return nodes;
    }

    public static ConsumerNode getConsumerNode(String nodeName, NodeCapacity capacity) {
        return new ConsumerNode(new MemberInfo(nodeName, 0, new ComponentKind[]{ComponentKind.Consumer}, capacity));
    }

    public static NodeCapacity getNodeCapacity(int qps, int throughputKbps) {
        return new NodeCapacity(qps, throughputKbps);
    }

    public static Assignment getAssignment(VaradhiSubscription subscription, ConsumerNode cn, SubscriptionUnitShard shard) {
        return new Assignment(subscription.getName(), shard.getShardId(), cn.getConsumerId());
    }

    public static ConsumerInfo getConsumerInfo(ConsumerNode node, VaradhiSubscription subscription, List<SubscriptionUnitShard> shards) {
        ConsumerInfo info = new ConsumerInfo(new ConcurrentHashMap<>(), node.getConsumerId(), node.getAvailable());
        shards.forEach(s -> info.addShardCapacity(subscription.getName(), s.getShardId(), s.getCapacityRequest()));
        return info;
    }
}
