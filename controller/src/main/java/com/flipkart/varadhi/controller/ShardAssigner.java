package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.controller.entities.Assignment;
import com.flipkart.varadhi.controller.entities.ConsumerNode;
import com.flipkart.varadhi.controller.impl.LeastAssignedStrategy;
import com.flipkart.varadhi.entities.SubscriptionShards;
import com.flipkart.varadhi.entities.SubscriptionUnitShard;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.mutable.MutableBoolean;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
public class ShardAssigner {
    private final AssignmentStrategy strategy;
    private final Map<String, ConsumerNode> consumerNodes;

    public ShardAssigner() {
        this.strategy = new LeastAssignedStrategy();
        this.consumerNodes = new ConcurrentHashMap<>();
    }

    public void addConsumerNodes(List<ConsumerNode> clusterConsumers) {
        clusterConsumers.forEach(c -> {
            addConsumerNode(c);
            log.info("Added consumer node {}", c.getMemberInfo().hostname());
        });
    }

    // Assign shards of single subscription to consumer nodes
    // TODO:: Should this take care of bulk mode (i.e. assign shards from multiple subscription -- node failure case)
    public List<Assignment> assignShard(List<SubscriptionUnitShard> shards, VaradhiSubscription subscription) {
        List<ConsumerNode> activeConsumers =
                consumerNodes.values().stream().filter(c -> !c.isMarkedForDeletion()).collect(Collectors.toList());
        log.info("AssignShards consumer nodes active:{} of total:{}", activeConsumers.size(), consumerNodes.size());
        return strategy.assign(shards, subscription, activeConsumers);
    }

    // returns shard assignments for the given set of shards if done (from persistent store).
    public List<Assignment> getShardsAssignment(SubscriptionShards shards, VaradhiSubscription subscription) {
        return new ArrayList<>();
    }

    public void consumerNodeJoined(ConsumerNode consumerNode) {
        boolean added = addConsumerNode(consumerNode);
        if (added) {
            log.info("ConsumerNode {} joined.", consumerNode.getMemberInfo().hostname());
        }
    }

    private boolean addConsumerNode(ConsumerNode consumerNode) {
        String consumerNodeId = consumerNode.getMemberInfo().hostname();
        MutableBoolean added = new MutableBoolean(false);
        consumerNodes.computeIfAbsent(consumerNodeId, k -> {
            added.setTrue();
            return consumerNode;
        });
        if (!added.booleanValue()) {
            log.warn("ConsumerNode {} already exists. Not adding again.", consumerNodes.get(consumerNodeId));
        }
        return added.booleanValue();
    }

    public void consumerNodeLeft(String consumerNodeId) {
        //TODO:: re-assign the shards (should this be trigger from here or from the controller) ?
        MutableBoolean marked = new MutableBoolean(false);
        consumerNodes.computeIfPresent(consumerNodeId, (k, v) -> {
            v.markForDeletion();
            marked.setTrue();
            return v;
        });
        if (marked.booleanValue()) {
            log.info("ConsumerNode {} marked for deletion.", consumerNodeId);
        } else {
            log.warn("ConsumerNode {} not found.", consumerNodeId);
        }
    }
}
