package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.entities.ConsumptionPolicy;
import com.flipkart.varadhi.entities.Endpoint;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.spi.services.TopicPartitions;

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
            Project project,
            String subscription,
            String shardName,
            TopicPartitions<StorageTopic> topic,
            boolean grouped,
            Endpoint endpoint,
            ConsumptionPolicy consumptionPolicy,
            ConsumptionFailurePolicy failurePolicy
    );

    CompletableFuture<Void> stopSubscription(String subscription, String shardName);

    void pauseSubscription(String subscription, String shardName);

    void resumeSubscription(String subscription, String shardName);

    ConsumerState getConsumerState(String subscription, String shardName);

    // TODO likely need status on the starting / stopping as well; as the above status is for a running consumer..
}
