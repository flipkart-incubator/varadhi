package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.core.cluster.ConsumerApi;
import com.flipkart.varadhi.entities.StorageSubscription;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.SubscriptionUnitShard;
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
    private final ConsumerInfo consumerInfo;


    public ConsumerApiMgr(ConsumersManager consumersManager, ConsumerInfo consumerInfo) {
        this.consumersManager = consumersManager;
        this.consumerInfo = consumerInfo;
    }

    @Override
    public CompletableFuture<Void> start(ShardOperation.StartData operation) {
        log.info("Consumer: Starting shard {}", operation);
        VaradhiSubscription subscription = operation.getSubscription();
        SubscriptionUnitShard shard = operation.getShard();
        consumerInfo.recordShardAssignment(subscription.getName(), shard.getShardId(), shard.getCapacityRequest());
        StorageSubscription<StorageTopic> mainSub = shard.getMainSubscription().getSubscriptionToConsume();
        ConsumptionFailurePolicy failurePolicy =
                new ConsumptionFailurePolicy(subscription.getRetryPolicy(), shard.getRetrySubscription(),
                        shard.getDeadLetterSubscription()
                );

        return consumersManager.startSubscription(
                subscription.getProject(),
                subscription.getName(),
                operation.getShardId(),
                mainSub,
                subscription.isGrouped(),
                subscription.getEndpoint(),
                subscription.getConsumptionPolicy(),
                failurePolicy
        );
    }

    @Override
    public CompletableFuture<Void> stop(ShardOperation.StopData operation) {
        VaradhiSubscription subscription = operation.getSubscription();
        SubscriptionUnitShard shard = operation.getShard();
        consumerInfo.purgeShardAssignment(subscription.getName(), shard.getShardId(), shard.getCapacityRequest());
        return consumersManager.stopSubscription(
                subscription.getName(),
                operation.getShardId()
        );
    }

    @Override
    public CompletableFuture<ShardStatus> getShardStatus(String subscriptionId, int shardId) {
        return CompletableFuture.completedFuture(new ShardStatus(ShardState.UNKNOWN, "Not a owner of shard"));
    }

    @Override
    public CompletableFuture<ConsumerInfo> getConsumerInfo() {
        return CompletableFuture.completedFuture(consumerInfo);
    }
}
