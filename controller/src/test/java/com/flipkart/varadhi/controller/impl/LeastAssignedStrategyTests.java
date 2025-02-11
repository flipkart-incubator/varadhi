package com.flipkart.varadhi.controller.impl;

import com.flipkart.varadhi.core.cluster.entities.NodeProvider;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.entities.cluster.Assignment;
import com.flipkart.varadhi.core.cluster.entities.ConsumerNode;
import com.flipkart.varadhi.core.cluster.entities.NodeCapacity;
import com.flipkart.varadhi.exceptions.CapacityException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class LeastAssignedStrategyTests {
    private LeastAssignedStrategy strategy;

    @BeforeEach
    public void setUp() {
        strategy = new LeastAssignedStrategy();
    }

    @Test
    public void testAssign_NoConsumerNodes_ThrowsException() {
        VaradhiSubscription subscription = SubscriptionUtils.builder()
                                                            .build("sub1", "subProject1", "subProject1.topic");
        List<SubscriptionUnitShard> shards = SubscriptionUtils.shardsOf(subscription);
        List<ConsumerNode> consumerNodes = Collections.emptyList();
        assertThrows(CapacityException.class, () -> strategy.assign(shards, subscription, consumerNodes));
    }

    @Test
    public void testAssign_EnoughResources_ReturnsAssignments() {
        VaradhiSubscription subscription = SubscriptionUtils.builder()
                                                            .setNumShards(2)
                                                            .build("sub1", "subProject1", "subProject1.topic");
        List<SubscriptionUnitShard> shards = SubscriptionUtils.shardsOf(subscription);
        List<ConsumerNode> nodes = NodeProvider.getConsumerNodes(2);
        NodeCapacity initialCapacity = nodes.get(0).getAvailable().clone();
        List<Assignment> assignments = strategy.assign(shards, subscription, nodes);
        assertEquals(2, assignments.size());
        assertEquals(shards.size(), assignments.size());
        assertNotEquals(assignments.get(0).getConsumerId(), assignments.get(1).getConsumerId());
        assertEquals(1, nodes.get(0).getAssignments().size());
        assertEquals(1, nodes.get(1).getAssignments().size());
        assertEquals(
            initialCapacity.getMaxQps() - shards.get(0).getCapacityRequest().getQps(),
            nodes.get(0).getAvailable().getMaxQps()
        );
    }

    @Test
    public void testAssign_NotEnoughResources_ThrowsException() {
        VaradhiSubscription subscription = SubscriptionUtils.builder()
                                                            .setNumShards(2)
                                                            .build("sub1", "subProject1", "subProject1.topic");
        List<SubscriptionUnitShard> shards = SubscriptionUtils.shardsOf(subscription);
        List<ConsumerNode> nodes = NodeProvider.getConsumerNodes(2, NodeProvider.getNodeCapacity(400, 10000));
        assertThrows(CapacityException.class, () -> strategy.assign(shards, subscription, nodes));
    }


    @Test
    public void testAssign_NotEnoughDistinctNodes_ReusesNodes() {
        // Create two shards and one consumer node with enough resources
        VaradhiSubscription subscription = SubscriptionUtils.builder()
                                                            .setNumShards(3)
                                                            .setCapacity(new TopicCapacityPolicy(12000, 15000, 2))
                                                            .build("sub1", "subProject1", "subProject1.topic");
        List<SubscriptionUnitShard> shards = SubscriptionUtils.shardsOf(subscription);
        List<ConsumerNode> nodes = new ArrayList<>();
        nodes.add(NodeProvider.getConsumerNode("node1", NodeProvider.getNodeCapacity(10000, 12000)));
        nodes.add(NodeProvider.getConsumerNode("node2", NodeProvider.getNodeCapacity(10000, 3000)));
        nodes.add(NodeProvider.getConsumerNode("node3", NodeProvider.getNodeCapacity(10000, 6000)));

        // Assign shards
        List<Assignment> assignments = strategy.assign(shards, subscription, nodes);

        // Check that both shards are assigned to the same node
        assertEquals(shards.size(), assignments.size());
        assertEquals(assignments.get(0).getConsumerId(), "node1");
        assertEquals(assignments.get(1).getConsumerId(), "node3");
        assertEquals(assignments.get(2).getConsumerId(), "node1");
    }

    @Test
    public void testAssignReusesAllNodesWhenNoAvailableNodes() {
        // Create two shards and one consumer node with enough resources
        VaradhiSubscription subscription = SubscriptionUtils.builder()
                                                            .setNumShards(4)
                                                            .setCapacity(new TopicCapacityPolicy(12000, 16000, 2))
                                                            .build("sub1", "subProject1", "subProject1.topic");
        List<SubscriptionUnitShard> shards = SubscriptionUtils.shardsOf(subscription);
        List<ConsumerNode> nodes = new ArrayList<>();
        nodes.add(NodeProvider.getConsumerNode("node1", NodeProvider.getNodeCapacity(10000, 100000)));
        nodes.add(NodeProvider.getConsumerNode("node2", NodeProvider.getNodeCapacity(10000, 100000)));

        // Assign shards
        List<Assignment> assignments = strategy.assign(shards, subscription, nodes);

        // Check that both shards are assigned to the same node
        assertEquals(shards.size(), assignments.size());
        assertEquals(assignments.get(0).getConsumerId(), "node1");
        assertEquals(assignments.get(1).getConsumerId(), "node2");
        assertEquals(assignments.get(2).getConsumerId(), "node1");
        assertEquals(assignments.get(3).getConsumerId(), "node2");
    }
}
