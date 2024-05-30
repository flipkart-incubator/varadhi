package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.core.cluster.ConsumerApi;
import com.flipkart.varadhi.entities.cluster.ConsumerInfo;
import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.cluster.ShardState;
import com.flipkart.varadhi.entities.cluster.ShardStatus;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

@Slf4j
public class ConsumerApiMgr implements ConsumerApi {
    private final ConsumersManager consumersManager;

    public ConsumerApiMgr(ConsumersManager consumersManager) {
        this.consumersManager = consumersManager;
    }

    @Override
    public CompletableFuture<Void> start(ShardOperation.StartData operation) {
        VaradhiSubscription subscription = operation.getSubscription();
        return consumersManager.startSubscription(
                null,
                subscription.getName(),
                "",
                null,
                subscription.isGrouped(),
                subscription.getEndpoint(),
                subscription.getConsumptionPolicy(),
                null
        );
    }

    @Override
    public CompletableFuture<Void> stop(ShardOperation.StopData operation) {
        VaradhiSubscription subscription = operation.getSubscription();
        return consumersManager.stopSubscription(
                subscription.getName(),
                ""
        );
    }

    @Override
    public CompletableFuture<ShardStatus> getShardStatus(String subscriptionId, int shardId) {
        return CompletableFuture.completedFuture(new ShardStatus(ShardState.UNKNOWN, "Not a owner of shard"));
    }

    @Override
    public CompletableFuture<ConsumerInfo> getConsumerInfo() {
        //TODO::Return assignments as well.
        return CompletableFuture.completedFuture(consumersManager.getInfo());
    }
}
