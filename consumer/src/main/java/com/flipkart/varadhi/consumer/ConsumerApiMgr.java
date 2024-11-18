package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.core.cluster.ConsumerApi;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.entities.cluster.*;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
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
                failurePolicy,
                shard.getCapacityRequest()
        );
    }

    @Override
    public CompletableFuture<Void> stop(ShardOperation.StopData operation) {
        log.info("Consumer: Stopping shard {}", operation);
        return consumersManager.stopSubscription(
                operation.getSubscriptionId(),
                operation.getShardId()
        );
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
    public CompletableFuture<List<Message>> getMessages(GetMessagesRequest messagesRequest) {
        //TODO::Along with implementation take care of (de)serialization format.
        //This should be byte array instead of string (check elsewhere as well for Message serialization)
        return CompletableFuture.supplyAsync(ArrayList::new);
    }
}
