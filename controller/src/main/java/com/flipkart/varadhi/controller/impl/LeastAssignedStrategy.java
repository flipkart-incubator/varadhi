package com.flipkart.varadhi.controller.impl;

import com.flipkart.varadhi.controller.AssignmentStrategy;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.entities.cluster.Assignment;
import com.flipkart.varadhi.core.cluster.ConsumerNode;
import com.flipkart.varadhi.entities.SubscriptionUnitShard;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.core.exceptions.CapacityException;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * LeastAssignedStrategy -- Allocate the shard to the consumer node which has the highest capacity available
 * or is least assigned. This kind of translates to uniform distributions of shards across all the nodes
 * (though not necessary)
 */
@Slf4j
public class LeastAssignedStrategy implements AssignmentStrategy {

    @Override
    public List<Assignment> assign(
        List<SubscriptionUnitShard> shards,
        VaradhiSubscription subscription,
        List<ConsumerNode> consumerNodes
    ) {
        if (consumerNodes.isEmpty()) {
            throw new CapacityException("No active consumer node for Subscription assignment.");
        }
        List<Assignment> assignments = new ArrayList<>();
        PriorityQueue<ConsumerNode> consumers = new PriorityQueue<>(
            Collections.reverseOrder(ConsumerNode.NodeComparator)
        );
        consumers.addAll(consumerNodes);
        List<ConsumerNode> usedNodes = new ArrayList<>();
        ArrayList<SubscriptionUnitShard> shardsToAssign = new ArrayList<>(shards);
        // Subscription's shards will be equal, so below sorting is no-op for now.
        shardsToAssign.sort(SubscriptionUnitShard.ShardCapacityComparator);

        int nextShardIndex = 1;
        for (SubscriptionUnitShard shard : shardsToAssign) {
            ConsumerNode consumerNode = consumers.remove();
            //TODO::handle for creating required capacity via re-assigning the shards.
            if (!consumerNode.canAllocate(shard.getCapacityRequest())) {
                log.error(
                    "Subscription:{} Shard:{} Assignment Failure: ResourcesNeeded:{}  max ResourcesAvailable on any node {}.",
                    subscription.getName(),
                    shard.getShardId(),
                    shard.getCapacityRequest(),
                    consumerNode.getAvailable()
                );
                throw new CapacityException("Not enough Resources for Subscription assignment.");
            }

            Assignment assignment = new Assignment(
                subscription.getName(),
                shard.getShardId(),
                consumerNode.getConsumerId()
            );
            consumerNode.allocate(assignment, shard.getCapacityRequest());
            assignments.add(assignment);

            // For the same subscription prefer next shard allocation on one of remaining consumer node.
            // However, reuse already allocated consumer nodes if assignment is not possible with remaining nodes.
            if (nextShardIndex < shardsToAssign.size()) {
                if (consumers.isEmpty()) {
                    // no other nodes, re-use current and excluded/used nodes.
                    consumers.addAll(usedNodes);
                    usedNodes.clear();
                    consumers.add(consumerNode);
                } else {
                    ConsumerNode nextHighestAvailable = consumers.peek();
                    TopicCapacityPolicy nextCapacityNeeded = shards.get(nextShardIndex).getCapacityRequest();
                    if (nextHighestAvailable.canAllocate(nextCapacityNeeded)) {
                        usedNodes.add(consumerNode);
                    } else {
                        // no nodes with enough capacity, re-use current and excluded/used nodes.
                        consumers.addAll(usedNodes);
                        usedNodes.clear();
                        consumers.add(consumerNode);
                    }
                }
                nextShardIndex++;
            }
        }
        return assignments;
    }
}
