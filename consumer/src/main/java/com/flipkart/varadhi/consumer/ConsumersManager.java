package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.entities.cluster.ConsumerState;

import java.util.Optional;
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
     * @return Future that will be completed when the consumer is started & ready to consume messages.
     */
    CompletableFuture<Void> startSubscription(
            String project, String subscription, int shardId, StorageSubscription<StorageTopic> mainSubscription,
            boolean grouped, Endpoint endpoint, ConsumptionPolicy consumptionPolicy,
            ConsumptionFailurePolicy failurePolicy, TopicCapacityPolicy capacityPolicy
    );

    CompletableFuture<Void> stopSubscription(String subscription, int shardId);

    void pauseSubscription(String subscription, int shardId);

    void resumeSubscription(String subscription, int shardId);

    /**
     * @param subscription
     * @param shardId
     * @return Optional.empty() if the shard is not being managed.
     */
    Optional<ConsumerState> getConsumerState(String subscription, int shardId);

    Iterable<Info> getConsumersInfo();

    record Info(String subscription, int shardId, ConsumerState state, TopicCapacityPolicy capacityPolicy) {
    }
}
