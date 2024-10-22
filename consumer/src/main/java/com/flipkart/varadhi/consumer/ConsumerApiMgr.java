package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.core.cluster.ConsumerApi;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.entities.cluster.ConsumerInfo;
import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.entities.cluster.ShardState;
import com.flipkart.varadhi.entities.cluster.ShardStatus;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ConsumerApiMgr implements ConsumerApi {
    private final ConsumersManager consumersManager;
    private final ConsumerInfo consumerInfo;

    //TODO:: removed or refactor shardMap appropriately when actual conumer implementation PR is merged.
    //This is to support testing for timme being.
    private final Map<String, ShardStatus> shardMap;


    public ConsumerApiMgr(ConsumersManager consumersManager, ConsumerInfo consumerInfo) {
        this.consumersManager = consumersManager;
        this.consumerInfo = consumerInfo;
        this.shardMap = new ConcurrentHashMap<>();
    }

    @Override
    public CompletableFuture<Void> start(ShardOperation.StartData operation) {
        log.info("Consumer: Starting shard {}", operation);
        SubscriptionUnitShard shard = operation.getShard();
        consumerInfo.addShardCapacity(operation.getSubscriptionId(), shard.getShardId(), shard.getCapacityRequest());
        StorageSubscription<StorageTopic> mainSub = shard.getMainSubscription().getSubscriptionToConsume();
        ConsumptionFailurePolicy failurePolicy =
                new ConsumptionFailurePolicy(operation.getRetryPolicy(), shard.getRetrySubscription(),
                        shard.getDeadLetterSubscription()
                );

        // TODO::Re-evalute interface w.r.to shardId object/record instead of project+subId+shardId.
        return consumersManager.startSubscription(
                operation.getProject(),
                operation.getSubscriptionId(),
                operation.getShardId(),
                mainSub,
                operation.isGrouped(),
                operation.getEndpoint(),
                operation.getConsumptionPolicy(),
                failurePolicy
        ).whenComplete((v, t) -> {
            String shardFqn = String.format("%s:%d", operation.getSubscriptionId(), operation.getShardId());
            ShardStatus status = new ShardStatus(
                    null == t ? ShardState.STARTED : ShardState.ERRORED,
                    null == t ? null : t.getMessage()
            );
            shardMap.put(shardFqn, status);
        });
    }

    @Override
    public CompletableFuture<Void> stop(ShardOperation.StopData operation) {
        log.info("Consumer: Stopping shard {}", operation);
        SubscriptionUnitShard shard = operation.getShard();
        consumerInfo.freeShardCapacity(operation.getSubscriptionId(), shard.getShardId(), shard.getCapacityRequest());
        return consumersManager.stopSubscription(
                operation.getSubscriptionId(),
                operation.getShardId()
        ).whenComplete((v, t) -> {
            String shardFqn = String.format("%s:%d", operation.getSubscriptionId(), operation.getShardId());
            if (null == t) {
                shardMap.remove(shardFqn);
            } else {
                shardMap.put(shardFqn, new ShardStatus(ShardState.ERRORED, t.getMessage()));
            }
        });
    }

    @Override
    public CompletableFuture<Void> unsideline(ShardOperation.UnsidelineData operation) {
        log.info("Consumer: unsideline  {}", operation);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<ShardStatus> getShardStatus(String subscriptionId, int shardId) {
        String shardFqn = String.format("%s:%d", subscriptionId, shardId);
        if (shardMap.containsKey(shardFqn)) {
            return CompletableFuture.completedFuture(shardMap.get(shardFqn));
        }
        return CompletableFuture.completedFuture(new ShardStatus(ShardState.UNKNOWN, "Not a owner of shard"));
    }

    @Override
    public CompletableFuture<ConsumerInfo> getConsumerInfo() {
        return CompletableFuture.completedFuture(consumerInfo);
    }

    @Override
    public CompletableFuture<List<Message>> getMessages(GetMessagesRequest messagesRequest) {
        //TODO::Along with implementation take care of (de)serialization format.
        // This should be byte array instead of string (check elsewhere as well for Message serialization)
        return CompletableFuture.supplyAsync(ArrayList::new);
    }
}
