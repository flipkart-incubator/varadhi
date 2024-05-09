package com.flipkart.varadhi.controller.impl;

import com.flipkart.varadhi.controller.AssignmentStrategy;
import com.flipkart.varadhi.controller.entities.Assignment;
import com.flipkart.varadhi.controller.entities.ConsumerNode;
import com.flipkart.varadhi.entities.SubscriptionUnitShard;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.exceptions.CapacityException;
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
            List<SubscriptionUnitShard> shards, VaradhiSubscription subscription, List<ConsumerNode> consumerNodes
    ) {
        Objects.requireNonNull(shards);
        if (consumerNodes.isEmpty()) {
            log.warn("Shard Assignment Failure: No active consumer nodes.");
            throw new CapacityException("No active consumer node for Subscription assignment.");
        }
        List<Assignment> assignments = new ArrayList<>();
        TreeSet<ConsumerNode> consumers = new TreeSet<>(Comparator.comparingInt(o -> o.getAvailable().getNicMBps()));
        consumers.addAll(consumerNodes);

        shards.forEach(shard -> {
                    ConsumerNode consumerNode = consumers.pollLast();
                    Objects.requireNonNull(consumerNode);
                    //TODO::handle for creating required capacity via re-assigning the shards.
                    if (shard.getRequests().getNicMBps() > consumerNode.getAvailable().getNicMBps()) {
                        log.warn(
                                "Shard Assignment Failure: ResourcesNeeded:{} Max ResourcesAvailable on any node {}.",
                                shard.getRequests(), consumerNode.getAvailable()
                        );
                        throw new CapacityException("Not enough Resources for Subscription assignment.");
                    }
                    Assignment assignment =
                            new Assignment(subscription.getName(), shard.getShardId(), consumerNode.getMemberInfo().hostname());
                    consumerNode.allocate(shard.getRequests());
                    assignments.add(assignment);
                    consumers.add(consumerNode);
                }
        );
        return assignments;
    }
}
