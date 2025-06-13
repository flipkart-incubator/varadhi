package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.core.cluster.consumer.ConsumerApi;
import com.flipkart.varadhi.core.cluster.ConsumerInfo;
import com.flipkart.varadhi.core.cluster.MemberInfo;
import com.flipkart.varadhi.core.subscription.ShardDlqMessageResponse;
import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.entities.cluster.*;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class ConsumerApiMgr implements ConsumerApi {

    private final ConsumersManager consumersManager;
    private final MemberInfo memberInfo;

    public ConsumerApiMgr(ConsumersManager consumersManager, MemberInfo memberInfo) {
        this.consumersManager = consumersManager;
        this.memberInfo = memberInfo;
    }

    @Override
    public CompletableFuture<Void> start(ShardOperation.StartData operation) {
        log.info("Consumer: Starting shard {}", operation);
        SubscriptionUnitShard shard = operation.getShard();

        StorageSubscription<StorageTopic> mainSub = shard.getMainSubscription().getSubscriptionForConsume();
        ConsumptionFailurePolicy failurePolicy = new ConsumptionFailurePolicy(
            operation.getRetryPolicy(),
            shard.getRetrySubscription(),
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
            failurePolicy,
            shard.getCapacityRequest()
        );
    }

    @Override
    public CompletableFuture<Void> stop(ShardOperation.StopData operation) {
        log.info("Consumer: Stopping shard {}", operation);
        return consumersManager.stopSubscription(operation.getSubscriptionId(), operation.getShardId());
    }

    @Override
    public CompletableFuture<Void> unsideline(ShardOperation.UnsidelineData operation) {
        log.info("Consumer: unsideline  {}", operation);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Optional<ConsumerState>> getConsumerState(String subscriptionId, int shardId) {
        var consumerState = consumersManager.getConsumerState(subscriptionId, shardId);
        return CompletableFuture.completedFuture(consumerState);
    }

    @Override
    public CompletableFuture<ConsumerInfo> getConsumerInfo() {
        ConsumerInfo info = ConsumerInfo.from(memberInfo);
        consumersManager.getConsumersInfo().forEach(i -> {
            info.addShardCapacity(i.subscription(), i.shardId(), i.capacityPolicy());
        });
        return CompletableFuture.completedFuture(info);
    }

    @Override
    public CompletableFuture<ShardDlqMessageResponse> getMessagesByTimestamp(long earliestFailedAt, int max_limit) {
        //TODO::Message serialization should be byte arrasdy
        log.info("Polled from earliestFailedAt: {}", earliestFailedAt);
        return CompletableFuture.supplyAsync(() -> new ShardDlqMessageResponse(new ArrayList<>(), null));
    }

    @Override
    public CompletableFuture<ShardDlqMessageResponse> getMessagesByOffset(String pageMarkers, int max_limit) {
        //TODO::Message serialization should be byte array
        log.info("Polled from pagerMarkers: {}", pageMarkers);
        return CompletableFuture.supplyAsync(() -> new ShardDlqMessageResponse(new ArrayList<>(), null));
    }
}
