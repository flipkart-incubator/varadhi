package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.controller.entities.Assignment;
import com.flipkart.varadhi.controller.entities.ConsumerNode;
import com.flipkart.varadhi.entities.SubscriptionUnitShard;
import com.flipkart.varadhi.entities.VaradhiSubscription;

import java.util.List;

public interface AssignmentStrategy {
    List<Assignment> assign(
            List<SubscriptionUnitShard> shards, VaradhiSubscription subscription, List<ConsumerNode> consumerNodes
    );
}
