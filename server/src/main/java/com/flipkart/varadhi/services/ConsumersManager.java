package com.flipkart.varadhi.services;

import com.flipkart.varadhi.consumer.ConsumerState;
import com.flipkart.varadhi.entities.ConsumptionPolicy;
import com.flipkart.varadhi.entities.Endpoint;
import com.flipkart.varadhi.entities.RetryPolicy;
import com.flipkart.varadhi.spi.services.TopicPartitions;
import io.vertx.core.Future;

/**
 * Management layer that will manage consumers for multiple subscriptions.
 */
public interface ConsumersManager {

    /**
     * Start consumption for a shard of a subscription. If subscription failed, the future will contain the exception
     * details.
     *
     * @param subscription
     *
     * @return
     */
    Future<Void> startSubscription(
            String subscription,
            int shardId,
            TopicPartitions<?> topics,
            boolean grouped,
            Endpoint endpoint,
            RetryPolicy retryPolicy,
            ConsumptionPolicy consumptionPolicy
    );

    Future<Void> stopSubscription(String subscription, int shardId);

    Future<Void> pauseSubscription(String subscription, int shardId);

    Future<Void> resumeSubscription(String subscription, int shardId);

    ConsumerState getConsumerState(String subscription, int shardId);

    // TODO likely need status on the starting / stopping as well; as the above status is for a running consumer..
}
