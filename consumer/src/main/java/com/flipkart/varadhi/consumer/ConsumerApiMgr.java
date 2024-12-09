package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.core.cluster.ConsumerApi;
import com.flipkart.varadhi.core.cluster.entities.ShardDlqMessageResponse;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.entities.cluster.*;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ConsumerApiMgr implements ConsumerApi {
    private final ConsumersManager consumersManager;
    private final ConsumerInfo consumerInfo;

    //TODO:: remove or refactor allocatedShards appropriately when actual consumer implementation PR is merged.
    //This is to support testing for time being.
    private final Map<String, AllocatedShard> allocatedShards;


    public ConsumerApiMgr(ConsumersManager consumersManager, ConsumerInfo consumerInfo) {
        this.consumersManager = consumersManager;
        this.consumerInfo = consumerInfo;
        this.allocatedShards = new ConcurrentHashMap<>();
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

        //TODO::Re-evaluate interface w.r.to shardId object/record instead of project+subId+shardId.
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
            allocatedShards.put(shardFqn, new AllocatedShard(shard, status));
        });
    }

    @Override
    public CompletableFuture<Void> stop(ShardOperation.StopData operation) {
        log.info("Consumer: Stopping shard {}", operation);
        String shardFqn = String.format("%s:%d", operation.getSubscriptionId(), operation.getShardId());
        AllocatedShard allocatedShard = allocatedShards.get(shardFqn);
        if (null == allocatedShard) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Not a owner of shard."));
        }
        consumerInfo.freeShardCapacity(
                operation.getSubscriptionId(), operation.getShardId(), allocatedShard.getShard().getCapacityRequest());
        return consumersManager.stopSubscription(
                operation.getSubscriptionId(),
                operation.getShardId()
        ).whenComplete((v, t) -> {
            if (null == t) {
                allocatedShards.remove(shardFqn);
            } else {
                allocatedShards.get(shardFqn).setStatus(new ShardStatus(ShardState.ERRORED, t.getMessage()));
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
        AllocatedShard allocatedShard = allocatedShards.get(shardFqn);
        if (null == allocatedShard) {
            return CompletableFuture.completedFuture(new ShardStatus(ShardState.UNKNOWN, "Not a owner of shard."));
        }
        return CompletableFuture.completedFuture(allocatedShard.getStatus());
    }

    @Override
    public CompletableFuture<ConsumerInfo> getConsumerInfo() {
        return CompletableFuture.completedFuture(consumerInfo);
    }

    @Override
    public CompletableFuture<ShardDlqMessageResponse> getMessagesByTimestamp(long earliestFailedAt, int max_limit) {
        //TODO::Message serialization should be byte arrasdy
        log.info("Polled from earliestFailedAt: {}",  earliestFailedAt);
        return CompletableFuture.supplyAsync(() -> new ShardDlqMessageResponse(new ArrayList<>(), null));
    }
    @Override
    public CompletableFuture<ShardDlqMessageResponse> getMessagesByOffset(String pageMarkers, int max_limit
    ) {
        //TODO::Message serialization should be byte array
        log.info("Polled from pagerMarkers: {}",  pageMarkers);
        return CompletableFuture.supplyAsync(() -> new ShardDlqMessageResponse(new ArrayList<>(), null));
    }
}
