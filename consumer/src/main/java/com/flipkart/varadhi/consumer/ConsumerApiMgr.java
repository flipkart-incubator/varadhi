package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.core.cluster.ConsumerApi;
import com.flipkart.varadhi.core.cluster.ControllerApi;
import com.flipkart.varadhi.core.cluster.ShardOperation;
import com.flipkart.varadhi.entities.SubscriptionShards;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConsumerApiMgr implements ConsumerApi {
    private final ConsumersManager consumersManager;

    public ConsumerApiMgr(ConsumersManager consumersManager) {
        this.consumersManager = consumersManager;
    }

    @Override
    public void start(ShardOperation.StartData operation) {
        VaradhiSubscription subscription = operation.getSubscription();
        SubscriptionShards shard = operation.getShard();
        consumersManager.startSubscription(
                null,
                subscription.getName(),
                "",
                null,
                subscription.isGrouped(),
                subscription.getEndpoint(),
                subscription.getConsumptionPolicy(),
                null
        );
        log.debug("Started subscription: {}", operation);
    }
}
