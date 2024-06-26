package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.entities.*;

import java.util.concurrent.CompletableFuture;

/**
 * Management layer that will manage consumers for multiple subscriptions.
 */
public interface ConsumersManager {

    /**
     * Start consumption for a shard of a subscription. If subscription failed, the future will contain the exception
     * details.
     * `shardName` identifies the different shards within the subscription.
     *
     * @return
     */
    CompletableFuture<Void> startSubscription(
            String project,
            String subscription,
            int shardId,
            StorageSubscription<StorageTopic> mainSubscription,
            boolean grouped,
            Endpoint endpoint,
            ConsumptionPolicy consumptionPolicy,
            ConsumptionFailurePolicy failurePolicy
    );

    CompletableFuture<Void> stopSubscription(String subscription, int shardId);

    void pauseSubscription(String subscription, int shardId);

    void resumeSubscription(String subscription, int shardId);

    ConsumerState getConsumerState(String subscription, int shardId);

    // TODO likely need status on the starting / stopping as well; as the above status is for a running consumer..
}
